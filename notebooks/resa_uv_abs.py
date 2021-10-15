import linecache
import os
import shutil
from pathlib import Path

import pandas as pd


def read_blank(blank_path):
    """Check that a blank file exists and read it to a dataframe.

    Args:
        blank_path: Str. Path to blank file

    Returns:
        Dataframe.
    """
    assert os.path.isfile(blank_path), f"Blank file not found at '{blank_path}'."

    blank_df = pd.read_csv(
        blank_path,
        delim_whitespace=True,
        skiprows=86,
        header=None,
        names=["wavelength", "blank_value"],
        index_col=0,
    )
    blank_df.index = blank_df.index.astype(int)

    assert (
        len(blank_df) == 701
    ), f"Blank file '{blank_path}' contains {len(blank_df)} rows (expected 701)."

    return blank_df


def get_year(fpath, line_num=4):
    """Get year from a raw sample file.

    Args:
        fpath:    Str. Path to raw file
        line_num: Int. Line to read containing date in format 'yy/mm/dd'

    Returns:
        Int. Year when analysis was conducted.
    """
    line = linecache.getline(fpath, line_num)
    year = int(line[:2]) + 2000

    return year


def read_uv_abs(fpath):
    """Read a raw UV absorbance file.

    Args:
        fpath: Str. Path to raw file

    Returns:
        Dataframe.
    """
    assert os.path.isfile(fpath), f"File not found at '{fpath}'."

    df = pd.read_csv(
        fpath,
        delim_whitespace=True,
        skiprows=86,
        header=None,
        names=["wavelength", "value"],
        index_col=0,
    )
    df.index = df.index.astype(int)

    assert len(df) == 701, f"File '{fpath}' contains {len(df)} rows (expected 701)."

    return df


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
    df = raw_abs_df.join(blank_df, how="inner")

    assert len(df) == 701, f"Joined dataframe contains {len(df)} rows (expected 701)."

    df["value"] = (df["value"] - df["blank_value"]) * dilution / cuvette_len_cm
    df = df[["value"]].reset_index()
    df["water_sample_id"] = ws_id
    df["method_id"] = meth_id

    return df


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
        raise ValueError(f"Found multiple RESA2 water sample IDs for {lw_txt_id}.")


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
    assert (
        len(df["water_sample_id"].unique()) == 1
    ), "Dataframe contains results for more than one sample."

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

        print(
            f"Successfully uploaded new data for NR-{year}-{serial_no} (water sample ID {ws_id})."
        )

    else:
        print(
            f"Skipping upload for NR-{year}-{serial_no} (water sample ID {ws_id}). "
            "Values already exist (use 'force_update=True' to reload)."
        )


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
