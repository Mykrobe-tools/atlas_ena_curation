# Atlas ENA Curation pipeline

Queries the ENA for all MTBC samples, retrieves related information , checks against sample names and hashes already in database, download reads for samples not in db, extracts metadata, imports newly acquired samples in db, and does the analysis and processing of those samples.

This pipeline is build on top of clockwork and scripts to download from the ENA written by Martin Hunt, as well as a COBS API and distance calculation scripts by Zhicheng Liu & Giang Nguyen.

***

## Installation

For the pipeline to work, nextflow (v0.28.0, at time of writing) and singularity are required.
Further installations may be required, such as the latest version of enaBrowserTools.
A clockwork database is also necessary for the pipeline to run.

The pipeline should be in a directory containing :
 - directory `bin/` with all pipeline scripts, including the [clockwork pipelines](https://github.com/iqbal-lab-org/clockwork) (`import.nf`, `remove_contam.nf`, `variant_call.nf`, `qc.nf`) and directory [mykrobe-atlas-distance-data](https://github.com/Mykrobe-tools/mykrobe-atlas-distance-data)
 - directories `Pipeline_root/`, `Pipeline_refs/` and `Pipeline_spreadsheet_archive/`, the clockwork database base directories
 - directory `data/`, where the reads are first downloaded
 - directory `metadata/`, where the metadata linked to the reads and mykrobe analysis summary will be stored
 - directory `predictor/`, where the mykrobe analysis results will be stored
 - directory `distance_data/`, where the distance calculations will be stored, including the current genotype calls and tree leaf gencalls
 - directory `cobs/`, where the cobs index will be built
 - latest clockwork container (`clockwork_container.v0.9.8.img`, at time of writing)
 - latest mykrobe container (`mykrobe.v0.10.0.img`, at time of writing)
 - latest kms container (`kms.sif`)
 - file db.ini for access to clockwork database
 - nextflow configuration file (requires use of lsf : `process.executor = 'lsf'`)
 - file `removed_samples.list`, might be an empty file or manually written samples you do not want included

## Check and change before using

Before using the pipeline, there might be some slight changes required for it to work.

1. Please make sure to check the parameters, notably :
 - `params.taxon_id`: "77643" is the ENA taxon ID for all MTBC. Use "115862" for a subset, better for testing purposes.
 - `params.ref_rc`: make sure to use the corresponding reference number from your database, name "remove_contam"
 - `params.ref_all`: make sure to use the corresponding reference number from your database, name "NC_000962.3"

2. Increase or lower value for `params.max_forks_myk`, used in processes `myk_cmd_provide` and `myk_prediction`.

3. Increase or lower value for `params.max_lines_gencalls`, used in processe `split_gencalls`.

4. Version in the regex of the process `find_ctx` paths.

## Usage

To launch the pipeline, use the following command :

```
nextflow run atlas_ena_curation.nf <arguments> [-work-dir]
```
