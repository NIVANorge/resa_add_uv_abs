import configparser
import getpass
import linecache
import logging
import os
import shutil
from datetime import datetime
from glob import glob
from pathlib import Path

import cx_Oracle
import numpy as np
import pandas as pd
import yagmail
from sqlalchemy import create_engine


def connect_to_nivabase():
    """Connect to the NIVABASE.
    Args:
        None.

    Returns:
        SQLAlchemy database engine.
    """
    user = getpass.getpass(prompt="Username: ")
    pw = getpass.getpass(prompt="Password: ")
    conn_str = f"oracle+cx_oracle://{user}:{pw}@nivabase.niva.no:1521/nivabase"

    engine = create_engine(conn_str)
    conn = engine.connect()
    print("Connection successful.")

    return engine


def get_analysis_datetime(fpath):
    """Get the date and time of an analysis from a raw sample or blank file.

    Args:
        fpath:    Str. Path to raw file

    Returns:
        Datetime object when analysis was conducted.
    """
    dt_tm = linecache.getline(fpath, 6) + " " + linecache.getline(fpath, 7)[:8]
    dt_tm = datetime.strptime(dt_tm, "%y/%m/%d %H:%M:%S")

    return dt_tm


def get_dilution(serial_no, year):
    """Get the dilution factor from Labware using the Labware text ID.

    Args:
        serial_no: Str. Zero-padded 5-digit Labware serial number.
                   e.g. the xxxxx part of 'NR-2019-xxxxx'
        year:      Int. Year in which analysis was run

    Returns:
        Int. Dilution factor for sample.
    """
    lw_txt_id = f"NR-{year}-{serial_no}"

    return 1


def get_water_sample_id(serial_no, year, engine):
    """Find the RESA2 water sample ID from the Labware text ID.

    Args:
        serial_no: Str. Zero-padded 5-digit Labware serial number.
                   e.g. the xxxxx part of 'NR-2019-xxxxx'
        year:      Int. Year in which analysis was run
        engine:    Obj. Active database connection object with access to
                   Nivabasen

    Returns:
        None or Int. Resa2 water sample ID (if found).
    """
    lw_txt_id = f"NR-{year}-{serial_no}"
    sql = "SELECT water_sample_id FROM resa2.labware_wsid WHERE labware_text_id=:lw_txt_id"
    df = pd.read_sql(sql, engine, params={"lw_txt_id": lw_txt_id})

    if len(df) == 0:
        return None
    elif len(df) == 1:
        ws_id = df["water_sample_id"].iloc[0]
        return int(ws_id)
    else:
        msg = f"ERROR: Found multiple RESA2 water sample IDs for {lw_txt_id}."
        raise ValueError(msg)


def read_uv_abs(fpath):
    """Read a raw UV absorbance file.

    Args:
        fpath: Str. Path to raw file

    Returns:
        Dataframe.
    """
    df = pd.read_csv(
        fpath,
        delim_whitespace=True,
        skiprows=86,
        header=None,
        names=["wavelength", "value"],
        index_col=0,
    )
    df.index = df.index.astype(int)

    if len(df) != 701:
        msg = f"ERROR: File '{fpath}' contains {len(df)} rows (expected 701)."
        raise ValueError(msg)

    return df


def assign_blanks(flist, blank_list):
    """Determine which blank file to use for each result file. An analysis will begin
       with a blank file (usually called 'BLANK.SP'), followed by several result files,
       then another blank file (usually 'BL.SP') and some more samples. The first blank
       file applies to the first set of results and the second to the second set of
       results. All the files include date & time stamps. This function uses these to
       determine which result files should use which blanks.

    Args:
        flist:      List of Str. List of paths to raw result files
        blank_list: List of Str. List of paths to blank files

    Returns:
        Dataframe with index equal to 'fpath' and columns 'datetime' and 'blank_path'.
    """
    # Read datetimes from result files
    data = {
        "fpath": flist,
        "datetime": [get_analysis_datetime(fpath) for fpath in flist],
    }
    fpath_df = pd.DataFrame(data).sort_values("datetime", ascending=True)

    # Read datetimes from blank files
    data = {
        "blank_path": blank_list,
        "datetime": [get_analysis_datetime(blank_path) for blank_path in blank_list],
    }
    blank_path_df = pd.DataFrame(data).sort_values("datetime", ascending=True)

    # Assign blanks to result files
    fpath_df["blank_path"] = pd.cut(
        fpath_df["datetime"],
        bins=blank_path_df["datetime"].tolist() + [datetime(2100, 1, 1)],
        labels=blank_path_df["blank_path"],
    )

    if pd.isna(fpath_df).sum().sum() > 0:
        msg = f"ERROR: Cannot assign blanks for all files:\n\n{fpath_df.to_string()}"
        raise LookupError(msg)

    fpath_df.set_index("fpath", inplace=True)

    return fpath_df


def correct_values(raw_abs_df, blank_df, cuvette_len_cm, dilution, ws_id, meth_id):
    """Adjust values in 'raw_abs_df' based on 'blank_df' and correct for
       cuvette length and dilution. Also adds water sample and method IDs.

    Args:
        raw_abs_df:     Dataframe. Raw UV absorbance values to correct. Must
                        have wavelength as an integer index
        blank_df:       Dataframe. Blank sample for this run. Must have
                        wavelength as an integer index
        cuvette_len_cm: Int. Length of cuvette used (in cm)
        dilution:       Int. Dilution factor used for sample (dimensionless)
        ws_id:          Int. Water sample ID in RESA for this UV abs analysis
        meth_id:        Int. Method ID in RESA corresponding to UV abs analysis

    Returns:
        Dataframe.
    """
    df = raw_abs_df.join(blank_df, how="inner", rsuffix="_blank")

    if len(df) != 701:
        msg = f"ERROR: Joined dataframe contains {len(df)} rows (expected 701)."
        raise ValueError(msg)

    df["value"] = (df["value"] - df["value_blank"]) * dilution / cuvette_len_cm
    df = df[["value"]].reset_index()
    df["water_sample_id"] = ws_id
    df["method_id"] = meth_id

    return df


def add_to_resa(
    df,
    fold,
    year,
    serial_no,
    blank_file,
    dilution,
    cuvette_len_cm,
    engine,
    force_update=False,
):
    """Adds corrected absorbance values for a single sample to
       RESA2.ABSORBANCE_SPECTRAS. Also logs when the sample was uploaded in
       RESA2.LOG_ABS_SPECTRA.

    Args:
        df:             Dataframe. Corrected values for a single water sample
        fold:           Str. Path to folder containing data being processed
        year:           Int. Year in which analysis was run
        serial_no:      Str. Zero-padded 5-digit Labware serial number.
                        e.g. the xxxxx part of 'NR-2019-xxxxx'
        blank_file:     Str. Either 'BL.SP' or 'BLANK.SP'
        dilution:       Int. Dilution factor used for sample (dimensionless)
        cuvette_len_cm: Int. Length of cuvette used (in cm)
        engine:         Obj. Active database connection object with access to
                        Nivabasen
        force_update:   Bool. Default False. If True, and values for this water
                        sample already exist in the database, they will be
                        deleted and uploaded again. If False, existing values
                        will remain unchanged and the the upload will be skipped

    Returns:
        None. Values are added to RESA.
    """
    ws_id = int(df["water_sample_id"].iloc[0])

    # Check if values already exist
    sql = "SELECT count(*) AS count FROM jes.absorbance_spectras WHERE water_sample_id = :ws_id"
    old_df = pd.read_sql(sql, engine, params={"ws_id": ws_id})
    old_count = old_df["count"].iloc[0]

    if (old_count == 0) or force_update:
        # Delete values for this sample if present
        sql = "DELETE FROM jes.absorbance_spectras WHERE water_sample_id = :ws_id"
        engine.execute(sql, {"ws_id": ws_id})

        # Add new data
        df.to_sql(
            name="absorbance_spectras",
            schema="jes",
            con=engine,
            index=False,
            if_exists="append",
        )

        # Log sprectra uploaded
        log_spectra_uploaded(
            fold, ws_id, year, serial_no, blank_file, dilution, cuvette_len_cm, engine
        )
        msg = f"Successfully uploaded new data for NR-{year}-{serial_no} (water sample ID {ws_id})."
        print(msg)
        logging.info(msg)

    else:
        msg = (
            f"Skipping upload for NR-{year}-{serial_no} (water sample ID {ws_id}). "
            "Values already exist (use 'force_update=True' to reload)."
        )
        print(msg)
        logging.info(msg)


def log_spectra_uploaded(
    fold, ws_id, year, serial_no, blank_file, dilution, cuvette_len_cm, engine
):
    """Log details of when samples were uploaded in RESA2.LOG_ABS_SPECTRA.

    Args:
        fold:           Str. Path to folder containing data being processed
        ws_id:          Int. Water sample ID in RESA for this UV abs analysis
        year:           Int. Year in which analysis was run
        serial_no:      Str. Zero-padded 5-digit Labware serial number.
                        e.g. the xxxxx part of 'NR-2019-xxxxx'
        blank_file:     Str. Either 'BL.SP' or 'BLANK.SP'
        dilution:       Int. Dilution factor used for sample (dimensionless)
        cuvette_len_cm: Int. Length of cuvette used (in cm)
        engine:         Obj. Active database connection object with access to
                        Nivabasen

    Returns:
        None. A row is added to RESA2.LOG_ABS_SPECTRA.
    """
    arch_path = os.path.join(fold, "uploaded")
    Path(arch_path).mkdir(parents=False, exist_ok=True)

    # blank_path = os.path.join(fold, blank_file)
    # blank_dest = os.path.join(arch_path, blank_file)
    # shutil.copyfile(blank_path, blank_dest)

    fpath = os.path.join(fold, f"{serial_no}.SP")
    shutil.move(fpath, arch_path)

    data = [
        f"NR-{year}-{serial_no}",
        ws_id,
        year,
        serial_no,
        blank_file,
        dilution,
        cuvette_len_cm,
        fpath,
        arch_path,
    ]
    sql = (
        "INSERT INTO jes.log_abs_spectra "
        "(labware_text_id, water_sample_id, year, serial_no, blank_file, dilution, cuvette_len_cm, original_path, archive_path) "
        "VALUES "
        "(:1, :2, :3, :4, :5, :6, :7, :8, :9)"
    )
    engine.execute(sql, data)


def send_email(to_list, subject, message, attach_list, auth_path=".auth"):
    """Send an e-mail from resa2.uvabs@gmail.com with the log file as an
       attachment.

       The authentication file is plain text and should have the format

            [Auth]
            email_user = resa2.uvabs
            email_pw = password

    Args:
        to_list:     List of Str. E-mail addresses to send to
        subject:     Str. E-mail subject
        message:     Str. E-mail body text
        attach_list: List of Str. List file paths for attachments
        auth_path:   Str. Path to authentication file containing username and
                     password for resa2.uvabs@gmail.com

    Returns:
        None. E-mail is sent.
    """
    config = configparser.RawConfigParser()
    config.read(auth_path)
    username = config.get("Auth", "email_user")
    password = config.get("Auth", "email_pw")

    yag = yagmail.SMTP(username, password)
    yag.send(to=to_list, subject=subject, contents=message, attachments=attach_list)


def main(
    uv_data_fold=r"../../test_data",
    force_update=False,
    cuvette_len_cm=5,
    meth_id=10666,
    log_fold=r"../logs/",
):
    """Main function for processing UV absorbance data."""
    log_date = datetime.today().strftime("%Y-%m-%d-%H-%M")
    log_file = os.path.join(log_fold, f"resa2_uvabs_log_{log_date}.txt")
    logging.basicConfig(
        filename=log_file, filemode="w", level=logging.DEBUG, format="%(message)s"
    )

    try:
        engine = connect_to_nivabase()

        # Relevant folder names begin with "AB"
        folders = glob(f"{uv_data_fold}/*")
        folders = [fold for fold in folders if os.path.split(fold)[1][:2] == "AB"]

        for fold in folders:
            blank_list = glob(f"{fold}/BL*.SP")
            flist = glob(f"{fold}/*.SP")
            flist = [fpath for fpath in flist if fpath not in blank_list]

            if (len(flist) > 0) and (len(blank_list) > 0):
                header = "############################################################################"
                msg = f"{header}\n{fold}\n{header}"
                print(msg)
                logging.info(msg)

                res_blank_df = assign_blanks(flist, blank_list)

                for fpath in flist:
                    serial_no = os.path.split(fpath)[1][:-3]
                    year = get_analysis_datetime(fpath).year
                    dilution = get_dilution(serial_no, year)
                    ws_id = get_water_sample_id(serial_no, year, engine)

                    if ws_id is None:
                        msg = (
                            f"Skipping upload for NR-{year}-{serial_no}. "
                            "Could not identify water sample in RESA2."
                        )
                        print(msg)
                        logging.info(msg)
                    else:
                        blank_path = res_blank_df.loc[fpath, "blank_path"]
                        blank_file = os.path.split(blank_path)[1]
                        df = read_uv_abs(fpath)
                        blank_df = read_uv_abs(blank_path)
                        df = correct_values(
                            df, blank_df, cuvette_len_cm, dilution, ws_id, meth_id
                        )
                        add_to_resa(
                            df,
                            fold,
                            year,
                            serial_no,
                            blank_file,
                            dilution,
                            cuvette_len_cm,
                            engine,
                            force_update=force_update,
                        )
    except Exception as e:
        print(e)
        logging.error(e)

    logging.shutdown()

    # Send e-mail report
    to_list = ["james.sample@niva.no"]
    subject = "RESA2 UV absorbance report"
    message = (
        f"Please find attached the log file for the latest RESA2 "
        f"UV absorbance data upload ({log_date})."
    )
    attach_list = [log_file]
    send_email(to_list, subject, message, attach_list)


if __name__ == "__main__":
    main()
