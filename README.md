# Uploading UV absorbance data to RESA2

A temporary solution for processing UV absorbance data from NIVA Lab's spectrophotometer.

The code here provides an alternative to Tore's old Access database (`ABSDATA_IMPORT.accdb`). This has previously been used to add absorbance data to RESA2, but it's fiddly and very slow.  The code in this repository is also rather rough, but it should be faster & more efficient than the existing workflow, and will hopefully provide an initial solution to a long-standing problem.

See the notebook [here](https://github.com/NIVANorge/resa_add_uv_abs/blob/main/notebooks/resa_add_uv_abs.ipynb) for details of the workflow.

**Note:** The issues in the issue tracker need to be addressed before this code can be used "for real". For development & testing, data are currently written to temporary tables within the `JES` schema of Nivabasen.