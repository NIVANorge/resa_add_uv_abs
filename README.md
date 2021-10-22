# Uploading UV absorbance data to RESA2

A temporary solution for processing UV absorbance data from NIVA Lab's spectrophotometer.

The code here provides an alternative to Tore's old Access database (`ABSDATA_IMPORT.accdb`). This has previously been used to add absorbance data to RESA2, but it's fiddly and very slow.  The code in this repository should be faster & more efficient, and will hopefully provide an initial solution to a long-standing problem. A comparison of results from the old and new workflows is [here](https://github.com/NIVANorge/resa_add_uv_abs/blob/main/notebooks/compare_old_and_new_methods.ipynb).

The code is designed to run automatically as a "scheduled task" on a machine within the NIVA network (i.e. with direct access to Nivabasen). It can also be run manually via the notebook [here](https://github.com/NIVANorge/resa_add_uv_abs/blob/main/notebooks/resa_add_uv_abs.ipynb) for greater control, such as when investigating if/when the automatic upload fails for some reason.

## Overview

UV absorbance analyses are conducted in batches, with one "blank"/calibration sample followed by a set of target samples. NIVA typically records absorbance for all integer wavelengths between 200 and 900 nm inclusive (i.e. 701 wavelengths per sample). Raw values from the target samples are corrected by subtracting the corresponding blank values. Corrections must also be made for dilution (e.g. where an unusually dark sample has been diluted before analysis) and cuvette length. The adjusted values to be uploaded to the database are calculated as:

$$A^{\lambda}_{cor} = \frac{D(A^{\lambda}_{raw} - A^{\lambda}_{blank})}{L}$$

where $A^{\lambda}_{cor}$ is the corrected value to be uploaded for wavelength $\lambda$ (in units of absorbance/cm); $A^{\lambda}_{raw}$ and $A^{\lambda}_{blank}$ are the raw and calibration absorbances at wavelength $\lambda$, respectively; $D$ is the dimensionless dilution factor; and $L$ is the cuvette length in cm.

The code in this repository reads the raw data, performs basic quality checking, calculates corrected values using the equation shown above, and uploads them to the RESA2 database. An e-mail is then automatically sent to selected recipients with a summary report listing the data uploaded and any issues requiring further attention.

## Detailed workflow

### For the Lab

**The workflow outlined below must be followed carefully for the code in this repository to work**. 

UV absorbance analyses are conducted by NIVALab using a Lambda 40P spectrophotometer. A blank file is run first, followed by a batch of samples, then another blank and some more samples etc. Several batches of (blank + results) may be produced each day. Results are automatically saved to the NIVA network at `T:/LAMBDA40P/UVWINLAB/DATA`, and each set of daily analyses are stored in a subfolder named `AB{yymmdd}` (e.g. `AB211018`). These folders preserve an original copy of the raw data. The code in this repository **does not** read these data directly, as we do not want to risk corrupting the originals if anything goes wrong.

After each analysis, Erling copies the `AB{yymmdd}` folders to `K:/Avdeling/412 Ana/LAMBDA/ABSSPEKTER_KAU`, ready for quality control by Liv Bente. For this new workflow, **I suggest creating a new folder at `K:/Avdeling/412 Ana/LAMBDA/ABSSPEKTER_AUTO_UPLOAD`**, which Erling can use from now on. Liv Bente will still be able to quality control the data (see "*Quality assurance*", below), and the code in this repository can also read data from this location without risking the original files.

Please note the following:

 * Folders must be named in the format `AB{yymmdd}` (e.g. `AB211018`) <br><br>
 
 * The first blank file must be run **before** the first set of samples. The code links blanks to samples based on the date and time of analysis recorded in the header of the output files. An error will be raised if there is no blank with a timestamp earlier than the first sample file <br><br>
 
 * The blank and sample files must both contain data for **701 wavelengths**. The code will raise an error if either dataset is incomplete <br><br>
 
 * The code only considers files with a `.SP` extension. Any other files will be ignored
 
### For Miljøinformatikk

The code in this repository should be deployed on a server with direct access to Nivabasen and a reasonably fast connection to `K:`. Data are uploaded to `RESA2.ABSORBANCE_SPECTRAS` and logged in `RESA2.LOG_ABS_SPECTRA`. 

In order for the upload to succeed, a valid water sample ID must already exist in RESA2 i.e. NIVALab must have already approved the general water chemistry analysis for the same sample being analysed on the spectrophotometer. The RESA2 water sample ID is found by matching the Labware text ID, which is constructed as `NR-{year}-{serial_no}`, where `year` is the year of analysis and `serial_no` is a zero-padded, five-digit code extracted from the filename (e.g. `09562.SP`). If the sample ID cannot be identified, a warning will be printed and the upload of data skipped until the next time the script runs (when it will try to identify the correct ID again).

Each sample that is successfully processed is moved to a subfolder named `uploaded`. For example, the file `.../AB211018/09562.SP` will be located at `.../AB211018/uploaded/09562.SP` once processing is complete. **Any files remaining long-term in the original top-level folder are therefore causing problems and should be investigated**.

The progress of the script, and any errors encountered, are written to a log file (and also printed to the terminal, if the script is run manually). After each upload, the log file is sent as an e-mail attachment to a selected list of recipients (from the e-mail address `resa2.uvabs@gmail.com`).

A typical log file will look like this (for a single folder):

    ############################################################################
    ../../test_data/AB210510
    ############################################################################
    Successfully uploaded new data for NR-2021-03222 (water sample ID 886041).
    Successfully uploaded new data for NR-2021-03543 (water sample ID 886294).
    Successfully uploaded new data for NR-2021-03544 (water sample ID 886295).
    Successfully uploaded new data for NR-2021-03545 (water sample ID 886306).
    Skipping upload for NR-2021-03548. Could not identify water sample in RESA2.
    Successfully uploaded new data for NR-2021-03549 (water sample ID 886297).
    Successfully uploaded new data for NR-2021-04036 (water sample ID 886029).
    Successfully uploaded new data for NR-2021-04037 (water sample ID 886030).

Common errors include e.g:

    ############################################################################
    ../../test_data/AB200916
    ############################################################################
    Successfully uploaded new data for NR-2020-07913 (water sample ID 876179).
    Successfully uploaded new data for NR-2020-08182 (water sample ID 876842).
    Successfully uploaded new data for NR-2020-08184 (water sample ID 875699).
    ERROR: File '../../test_data/AB200916/08188.SP' contains 699 rows (expected 701).
    
I have tried to make the script produce helpful error messages so that data issues can be traced and fixed by people outside of Miljøinformatikk as far as possible.

Each time the script runs, it scans **all** folders named `AB{yymmdd}` within the parent folder. This is because water sample IDs that cannot be identified one week may be identifiable the next, due to the lab finalising water chemistry analyses.

Sometimes samples are reanalysed, so results from the same sample can be provided twice in different batches. In this case, the log file will show a warning like:

    ############################################################################
    ../../test_data/AB170609
    ############################################################################
    Skipping upload for NR-2017-01140 (water sample ID 657524). Values already exist (use 'force_update=True' to reload).
    Successfully uploaded new data for NR-2017-03924 (water sample ID 658120).
    Successfully uploaded new data for NR-2017-03925 (water sample ID 658118).
    
and skip over the duplicated file. If you are sure the second set of values is correct (i.e. a reanalysis rather than any other error), the script can be run again manually with `force_update = True`. This will delete any existing values for this sample in the database and replace them with the new ones.

Further details can be found in the notebook [here](https://github.com/NIVANorge/resa_add_uv_abs/blob/main/notebooks/resa_add_uv_abs.ipynb) and the code [here](https://github.com/NIVANorge/resa_add_uv_abs/blob/main/notebooks/resa_uv_abs.py).

### For quality assurance

The log files described above provide more detailed status and error information than the previous Access database. The log files should be checked and issues followed-up and corrected as soon as possible. Once the result files are valid, the upload should take place successfully next time the script runs.

Please pay particular attention to the following log file messages:

 1. `Skipping upload for NR-yyyy-xxxxx. Could not identify water sample in RESA2`<br> 
    *This is just a warning, not an error*. This is expected for recently processed samples, where the water chemistry analysis has not yet been finalised by the lab. In this case, it is OK to simply wait and the sample should be processed as soon as an ID can be correctly identified. However, if this warning is being printed for old samples (i.e. long after other analyses on the same sample have finished), it should be investigated to determine the cause <br><br>
    
 2. `Skipping upload for NR-yyyy-xxxxx (water sample ID xxxxxx). Values already exist (use 'force_update=True' to reload)`<br>
     *This is a warning, not an error*. If this is a reanalysis, the more recent set of values should probably replace the first. In this case, the script can be re-run manually with `force_update=True` to update the values in the database (contact James Sample). Otherwise, there may be an error/conflict with sample IDs, which should be investigated <br><br>
     
 3. `ERROR: File '../../test_data/AByymmdd/xxxxx.SP' contains 699 rows (expected 701)`<br>
    *This is an error*. The file should be checked and the sample reanalysed if necessary <br><br>
    
 4. `ERROR: Cannot assign blanks for all files`<br>
    *This is an error*. It will be followed by a table of result files for which blanks cannot be found. The most likely cause is that a blank sample was not run **before** the main set of samples. In this case, the folder should be manually checked to identify an appropriate blank (if present) or reanalysed if this is not possible. If a blank can be identified manually, it can be copied and the timestamp in the header adjusted so that it falls before the first true sample. The batch will then be processed successfully when the script next runs 
    
As noted in the section above, each sample that is successfully processed is moved to a subfolder named `uploaded`. For example, the file `.../AB211018/09562.SP` will be located at `.../AB211018/uploaded/09562.SP` once processing is complete. **Any files remaining long-term in the original top-level folder are therefore causing problems and should be investigated**. These files will be highlighted in the log files by one of the warning or error messages listed above.


{% include mathjax.html %}