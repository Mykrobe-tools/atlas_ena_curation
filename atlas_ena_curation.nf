#!/usr/local/bin nextflow

// parameters
params.help = false

params.taxon_id = 77643 //use this taxon_id 115862 instead for testing purposes
params.ref_rc = 1 //change for value in db
params.ref_all = 2 //change for value in db
params.max_forks_myk = 100 //increase or lower if need be
params.max_lines_gencalls = 1000 //increase or lower if need be

params.pipeline_root = "$baseDir/Pipeline_root/"
params.pipeline_refs = "$baseDir/Pipeline_refs/"
params.xlsx_archive_dir = "$baseDir/Pipeline_spreadsheet_archive/"
params.dropbox_dir = "$baseDir/data/"
params.preddir = "$baseDir/predictor/"
params.distance_data = "$baseDir/distance_data/"
params.metadata_dir = "$baseDir/metadata/"

params.clockwork_img = "$baseDir/clockwork_container.v0.9.8.img"
params.mykrobe_img = "$baseDir/mykrobe.v0.10.0.img"
params.kms_img = "$baseDir/kms.sif"

params.db_config_file = "$baseDir/db.ini"
params.import_sp = "$baseDir/data/import.tsv"
params.distance_bin = "$baseDir/bin/mykrobe-atlas-distance-data/" //do not forget to import this!
params.removed_samples = "$baseDir/bin/removed_samples.list"
params.nf_config = "$baseDir/nextflow.config"
params.leaf_gencalls = "$baseDir/distance_data/leaf_gencalls"

// parameters into variables
dataset_name = "ENA"

pipeline_root = file(params.pipeline_root).toAbsolutePath()
pipeline_refs = file(params.pipeline_refs).toAbsolutePath()
xlsx_archive_dir = file(params.xlsx_archive_dir).toAbsolutePath()
dropbox_dir = file(params.dropbox_dir).toAbsolutePath()
preddir = file(params.preddir).toAbsolutePath()
distance_data = file(params.distance_data).toAbsolutePath()
metadata_dir = file(params.metadata_dir).toAbsolutePath()

clockwork_img = file(params.clockwork_img).toAbsolutePath()
mykrobe_img = file(params.mykrobe_img).toAbsolutePath()
kms_img = file(params.kms_img).toAbsolutePath()

db_config_file = file(params.db_config_file)
import_sp = file(params.import_sp).toAbsolutePath()
distance_bin = file(params.distance_bin).toAbsolutePath()
removed_samples = file(params.removed_samples).toAbsolutePath()
nf_config = file(params.nf_config).toAbsolutePath()
leaf_gencalls = file(params.leaf_gencalls).toAbsolutePath()

println """\
            ===================================
              _   _             ______ _   _          
         /\\  | | | |           |  ____| \\ | |   /\\    
        /  \\ | |_| | __ _ ___  | |__  |  \\| |  /  \\   
       / /\\ \\| __| |/ _` / __| |  __| | . ` | / /\\ \\  
      / ____ \\ |_| | (_| \\__ \\ | |____| |\\  |/ ____ \\ 
     /_/    \\_\\__|_|\\__,_|___/_|______|_| \\_/_/    \\_\\
                             | | (_)                  
          ___ _   _ _ __ __ _| |_ _  ___  _ __        
         / __| | | | '__/ _` | __| |/ _ \\| '_ \\       
        | (__| |_| | | | (_| | |_| | (_) | | | |      
         \\___|\\__,_|_|  \\__,_|\\__|_|\\___/|_| |_|   
                                                        
            ===================================
        """
        .stripIndent()

if (params.help){
    log.info"""
        Clockwork ENA download pipeline of samples
        with taxon id 77643 (Mycobacterium tuberculosis complex).

        Usage : nextflow run atlas_ena_curation.nf <arguments> [-work-dir]

        Pipeline should be launched from a directory containing :
            - dir bin/ with all pipeline scripts, including clockwork
                pipelines and dir mykrobe-atlas-distance-data
            - clockwork db dir Pipeline_root/
            - clockwork db dir Pipeline_refs/
            - clockwork db dir Pipeline_spreadsheet_archive/
            - dir data/
            - dir metadata/
            - dir predictor/
            - dir distance_data/
            - dir cobs/
            - latest clockwork container
            - latest mykrobe container
            - latest kms container
            - file db.ini for access to database
            - nextflow configuration file named nextflow.config
            - file removed_samples.list

        Please be sure to verify if following parameters fit,
        and change accordingly :
            - taxon_id
            - ref_rc
            - ref_all
            - max_forks_myk
            - max_lines_gencalls
            - process find_ctx file path regex
        
        For more detailed explanations, refer to README.md
    """.stripIndent()

    exit 0
}

////////////////////////////////////////////////////////////////////////////

process retrieve_ena_data {
    // Query ENA for all samples that are taxon id 77643, or children of 77643,
    // and parse into TSV file
    // Test taxon id 115862, part of MTBC taxon, 72 samples
    
    output:
    file 'samples.tsv' into samples_channel

    """
    curl -X POST --header 'Content-Type: application/x-www-form-urlencoded' --header 'Accept: text/plain' -d 'result=sample&query=tax_tree(${params.taxon_id})&format=tsv' 'https://www.ebi.ac.uk/ena/portal/api/search' > samples.tsv
    """
}

process samples_to_project_and_runs {
    // Create a table with study, sample and runs accession IDs, as well
    // as md5 for each

    input :
    file samples_tsv from samples_channel

    output:
    file 'study.sample.runs.temp.tsv' into study_sample_runs_temp_channel

    """
    samples_to_project_and_runs.py $samples_tsv
    """
}

process get_sample_names_archive {
    // Get names of samples already in the database as a channel

    output :
    file 'sample_db_archive.tsv' into sample_archive_channel

    """
    db_get_sample_names.py ${db_config_file} ${removed_samples}
    """
}

process check_sample_names {
    // Remove duplicates already in the database by comparing to samples'
    // names in the database
    
    input :
    file sample_archive from sample_archive_channel
    file temp_tsv from study_sample_runs_temp_channel

    output :
    stdout into name_check
    val true into name_check_channel
    file 'study.sample.runs.temp2.tsv' into study_sample_runs_temp2_channel

    """
    check_sample_names.py ${sample_archive} ${temp_tsv}
    """
}

name_check.println

process get_md5_archive {
    // Get md5 of samples already in the database as a channel

    output :
    file 'md5_archive.tsv' into md5_archive_channel

    """
    db_get_md5.py ${db_config_file}
    """
}

process check_md5sum {
    // Check against all md5 hashes, and remove sample if there is a match
    // in the database
    
    input :
    file md5_archive from md5_archive_channel
    file temp_tsv from study_sample_runs_temp2_channel

    output :
    val true into md5_check_channel
    stdout into md5_check_result
    file 'study.sample.runs.tsv' into study_sample_runs_channel

    """
    check_md5.py ${temp_tsv} ${md5_archive}
    """
}

md5_check_result.println

process make_ena_download_files {
    // Populate directory with several 2-col tables with sample ID and
    // corresponding run IDs (max 5000 samples per file)
    
    input :
    val md5_check from md5_check_channel
    file study_sample_runs_tsv from study_sample_runs_channel

    output :
    file '*.tsv' into dl_prep_channel
    stdout into study_nb

    """
    make_ena_download_files.py $study_sample_runs_tsv
    """
}

study_nb.println { it.trim() }

process get_date {
    // Get the date for versioning

    output :
    stdout into date

    """
    date +'%Y%m%d' | tr -d '[:cntrl:]'
    """
}

process get_time {
    // Get the time for versioning (to use instead of date if pipeline
    // running several times a day)

    output :
    stdout into time

    """
    date +'%Y%m%d%T' | tr -d '[:cntrl:],:'
    """
}

process ena_download {
    // Run clockwork pipeline to download fastq files, and calculate their md5 hashes
    // alongside a table containing samples information.
    errorStrategy 'retry'
    maxRetries 3

    publishDir dropbox_dir

    input :
    file tsvs from dl_prep_channel
    val date

    output :
    file "import_temp.tsv" into import_temp_channel

    """
    singularity exec ${clockwork_img} clockwork ena_download $tsvs ${dropbox_dir} site lab $date atlas_pilot
    mv ${dropbox_dir}/import.tsv ${dropbox_dir}/import_temp.tsv
    rsync -arvc ${dropbox_dir}/import_temp.tsv ./
    """
}

process check_md5sum_after_dl {
    // Check against all md5 hashes, and removes sample if there is a match
    // in the database
    
    input :
    file md5_archive from md5_archive_channel
    file import_temp from import_temp_channel

    output :
    val true into md5_check2_channel
    file "delete_reads.sh" into delete_channel

    """
    check_md5_after_dl.py ${import_temp} ${md5_archive} ${dropbox_dir}
    mv ./import.tsv ${dropbox_dir}/import.tsv
    """
}


process delete_files {
    // Delete potential duplicates based on md5 hashes of downloaded files

    input :
    val md5_check2 from md5_check2_channel
    file delete_files from delete_channel

    output :
    val true into delete_check_channel
    val true into delete_check_channel2
    val true into delete_check_channel3
    stdout into delete_check_result

    """
    bash $delete_files
    """
}

delete_check_result.println

process extract_metadata {
    // Query BioSample for all new samples' available metadata
    // (upload center, collection date, geo loc, isolate source, patient status...)

    publishDir metadata, mode : 'move'

    input :
    val delete_check from delete_check_channel
    val date

    output :
    file "ena_metadata_${date}.tsv" into metadata_channel
    stdout into metadata_result

    """
    ena_metadata_extractor.py ${dropbox_dir}/import.tsv $date
    """
}

metadata_result.println

process myk_cmd_provide {
    // Provide script with mykrobe commands (updated for v0.10.0)

    input :
    val delete_check from delete_check_channel2
    val date

    output :
    file "myk_predictor_*.sh" into myk_cmd_channel
    file "sample.list.tsv" into myk_list_channel
    file "sample.list" into sample_list_channel

    """
    predicting_cmd_provider.py ${import_sp} ${dropbox_dir} ${dataset_name} ${date} ${preddir} ${mykrobe_img}
    split -d --lines=${params.max_forks_myk} myk_predictor.sh myk_predictor_*.sh
    rm myk_predictor.sh
    """
}

process myk_prediction {
    // Run mykrobe predictions on raw reads
    maxForks params.max_forks_myk
    memory '8 GB'

    input :
    file myk_cmd from myk_cmd_channel.flatten()

    output :
    val true into mykrobe_check_channel
    val true into mykrobe_check_channel2

    """
    bash ${myk_cmd} && echo "[DEBUG] $myk_cmd success" || echo "[DEBUG] $myk_cmd fail"
    """
}

process get_myk_analysis_summary {
    // Get a summary of mykrobe analysis for all new samples in one TSV file

    publishDir metadata, mode : 'move'

    input :
    val mykrobe_check from mykrobe_check_channel.collect()
    val date
    file json_path from myk_list_channel

    output :
    file "ena_myk_analysis_${date}.tsv" into myk_analysis_channel

    """
    get_lin_res_mut.py $json_path $date
    """

}

process pre_import_data {
    // Prepare script for import of data to the database

    input :
    val delete_check from delete_check_channel3

    output :
    file "import.sh" into import_channel

    """
    echo "nextflow run $baseDir/bin/import.nf -with-singularity ${clockwork_img} --dropbox_dir ${dropbox_dir} --pipeline_root ${pipeline_root} --xlsx_archive_dir ${xlsx_archive_dir} --db_config_file ${db_config_file}" > import.sh
    """
}

process import_data {
    // Import new samples to database

    input :
    file import_script from import_channel
    val mykrobe_check from mykrobe_check_channel2.collect()

    output :
    val true into imported_check_channel
    val true into imported_check_channel2

    """
    bash $import_script
    """
}


process remove_contam {
    // Run the clockwork contamination removal pipeline
    memory '9 GB'

    input :
    val import_check from imported_check_channel

    output :
    val true into rc_check_channel
    val true into rc_check_channel2

    """
    nextflow run $baseDir/bin/remove_contam.nf \
    -with-singularity ${clockwork_img} \
    --ref_id ${params.ref_rc} --references_root ${pipeline_refs} \
    --pipeline_root ${pipeline_root} \
    --db_config_file ${db_config_file} \
    -c ${nf_config}
    """
}

process variant_call {
    // Run the clockwork variant calling pipeline
    memory '12 GB'

    input :
    val rc_check from rc_check_channel

    output :
    val true into vc_check_channel


    """
    nextflow run $baseDir/bin/variant_call.nf \
    -with-singularity ${clockwork_img} \
    --ref_id ${params.ref_all} --references_root ${pipeline_refs} \
    --pipeline_root ${pipeline_root} \
    --db_config_file ${db_config_file} \
    -c ${nf_config}
    """
}

process qc {
    // Run the clockwork QC pipeline
    memory '4 GB'

    input :
    val rc_check from rc_check_channel2

    output :
    val true into qc_check_channel

    """
    nextflow run $baseDir/bin/qc.nf \
    -with-singularity ${clockwork_img} \
    --ref_id ${params.ref_all} --references_root ${pipeline_refs} \
    --pipeline_root ${pipeline_root} \
    --db_config_file ${db_config_file} \
    -c ${nf_config}
    """
}

process find_ctx {
    // Grab all cortex files from atlas samples

    input :
    val vc_check from vc_check_channel

    output :
    file "list_ctx.txt" into find_ctx_channel

    """
    find $pipeline_root -wholename "*/variant_call/0.9.0*/cortex/cortex.out/binaries/cleaned/k31/*.ctx" > list_ctx.txt
    """
}

process get_new_sample_info {
    // Select cortex files of new samples if they passed QC

    input :
    val qc_check from qc_check_channel

    output :
    file "new_sample_info.tsv" into new_sample_info_channel
    file "qc_discarded_samples" into qc_discarded_samples_channel

    """
    db_get_new_sample_info.py ${db_config_file}
    """
}

process make_ctx_tsvs {
    // Create the two following files :
    // TSV with sample_name, sample_id and cortex file path for the newly added samples
    // Tab seperated input file for building of cobs index (format <sample_name> <ctx_path>, no header)

    input :
    file new_samples_info from new_sample_info_channel
    file list_ctx from find_ctx_channel

    output :
    file "new_samples_ctx.tsv" into new_samples_ctx_channel
    file "cobs_input.tsv" into cobs_input_channel

    """
    make_ctx_tsv.py ${new_samples_info} ${list_ctx}
    """
}

process build_cobs {
    // Building a cobs index for new samples

    input :
    file cobs_input from cobs_input_channel
    val date

    output :
    val true into cobs_channel

    """
    nextflow run $baseDir/bin/build_cobs_index.nf -c ${nf_config}\
    --samples ${cobs_input} --image ${kms_img} --outputDir $baseDir/cobs/$date/
    """
}

process call_genotypes {
    // Call genotypes for new samples

    input :
    val cobs_flag from cobs_channel
    file sample_list from sample_list_channel
    val date

    output :
    file "gencalls_${date}" into new_gencalls_channel
    file "gencalls_${date}" into new_gencalls_channel2
    file "gencalls_${date}" into new_gencalls_channel3

    """
    nextflow run $distance_bin/nextflow/cobs_query_probes.nf \
    --probes $distance_bin/probes/probes.fa \
    --image ${kms_img} \
    --classic_index_dir $baseDir/cobs/$date/merged/index/ \
    --sample_list ${sample_list} \
    --output_genotype_call_path gencalls_$date \
    -c ${nf_config}
    """
}

process make_gencalls_backup {
    // Create a backup of the genotype calls matrix of all samples added before that date

    //publishDir distance_data, mode : 'move'

    input :
    file new_gencalls from new_gencalls_channel
    val date

    output :
    val true into gencalls_flag

    """
    rsync -arvc $distance_data/all_gencalls $distance_data/all_gencalls_${date}.backup
    cat ${new_gencalls} >> $distance_data/all_gencalls
    """
}

process split_gencalls {
    // In case there is a large number of new samples, parallelise sample matrix creation

    input :
    file new_gencalls from new_gencalls_channel2

    output :
    file "split_gencalls_*" into split_gencalls_channel

    """
    split -d --lines=1 ${new_gencalls} split_gencalls_
    """
}

process make_sample_matrix {
    // Creates sample matrix of new samples against all samples

    input :
    file split_gencalls from split_gencalls_channel.flatten()
    val flag from gencalls_flag
    val date


    output :
    file "sub_dist_matrix_${date}" into sub_dist_matrix_channel

    """
    python3 $distance_bin/nextflow/bin/calculate_distance.py \
    --genotype-calls1 ${split_gencalls} \
    --genotype-calls2 $distance_data/all_gencalls \
    --out-distances sub_dist_matrix_$date
    """
}

process merge_sub_matrix {
    // Creates sample matrix of new samples against all samples

    publishDir distance_data, mode : 'copy'

    input :
    file sub_matrix_collection from sub_dist_matrix_channel.collect()
    val date

    output :
    file "distance_matrix_${date}" into dist_matrix_channel
    file "distance_matrix_${date}" into dist_matrix_channel2
    file "distance_matrix_${date}" into dist_matrix_channel3
    file "distance_matrix_${date}" into dist_matrix_channel4
    file "distance_matrix_${date}" into dist_matrix_channel5

    """
    ((CNT=1))

    for sub_matrix in ${sub_matrix_collection}
    do
        if (($CNT==1)); then head -1 \$sub_matrix > distance_matrix_$date
        fi
        tail -n +2 \$sub_matrix >> distance_matrix_$date
        ((CNT+=1))
    done
    """
}

process make_leaf_matrix {
    // Creates leaf matrix

    publishDir distance_data, mode : 'copy'

    input :
    file new_gencalls from new_gencalls_channel3
    val date

    output :
    file "leaf_matrix_${date}" into leaf_matrix_channel
    file "leaf_matrix_${date}" into leaf_matrix_channel2
    file "leaf_matrix_${date}" into leaf_matrix_channel3
    file "leaf_matrix_${date}" into leaf_matrix_channel4
    file "leaf_matrix_${date}" into leaf_matrix_channel5
    file "leaf_matrix_${date}" into leaf_matrix_channel6

    """
    python3 $distance_bin/nextflow/bin/calculate_distance.py \
    --genotype-calls1 ${new_gencalls} \
    --genotype-calls2 ${leaf_gencalls} \
    --out-distances leaf_matrix_$date
    """
}

process make_nearest_leaf {
    // Creates list of nearest tree leaf for each sample

    publishDir distance_data, mode : 'move'

    input :
    file leaf_matrix from leaf_matrix_channel
    val date

    output :
    file "nearest_leaf_${date}" into nearest_leaf_channel

    """
    python3 $distance_bin/nextflow/bin/generate_nearest_leaf.py \
    --distance-matrix-sample-tree ${leaf_matrix} > nearest_leaf_$date
    """
}

process make_nearest_neighbours_6 {
    // Creates nearest neighbours with a threshold of 6

    publishDir distance_data, mode : 'move'

    input :
    file leaf_matrix from leaf_matrix_channel2
    file dist_matrix from dist_matrix_channel
    val date

    output :
    file "nn_thresh6_${date}" into nn6_channel

    """
    python3 $distance_bin/nextflow/bin/generate_nearest_neighbours.py \
    --distance-matrix-sample-sample ${dist_matrix} \
    --distance-matrix-sample-tree ${leaf_matrix} \
    --distance-threshold 6
    > ./nn_thresh6_$date
    """
}

process make_nearest_neighbours_8 {
    // Creates nearest neighbours with a threshold of 8

    publishDir distance_data, mode : 'move'

    input :
    file leaf_matrix from leaf_matrix_channel3
    file dist_matrix from dist_matrix_channel2
    val date

    output :
    file "nn_thresh8_${date}" into nn8_channel

    """
    python3 $distance_bin/nextflow/bin/generate_nearest_neighbours.py \
    --distance-matrix-sample-sample ${dist_matrix} \
    --distance-matrix-sample-tree ${leaf_matrix} \
    --distance-threshold 8
    > ./nn_thresh8_$date
    """
}

process make_nearest_neighbours_10 {
    // Creates nearest neighbours with a threshold of 10

    publishDir distance_data, mode : 'move'

    input :
    file leaf_matrix from leaf_matrix_channel4
    file dist_matrix from dist_matrix_channel3
    val date

    output :
    file "nn_thresh10_${date}" into nn10_channel

    """
    python3 $distance_bin/nextflow/bin/generate_nearest_neighbours.py \
    --distance-matrix-sample-sample ${dist_matrix} \
    --distance-matrix-sample-tree ${leaf_matrix} \
    --distance-threshold 10
    > ./nn_thresh10_$date
    """
}

process make_nearest_neighbours_15 {
    // Creates nearest neighbours with a threshold of 15

    publishDir distance_data, mode : 'move'

    input :
    file leaf_matrix from leaf_matrix_channel5
    file dist_matrix from dist_matrix_channel4
    val date

    output :
    file "nn_thresh15_${date}" into nn15_channel

    """
    python3 $distance_bin/nextflow/bin/generate_nearest_neighbours.py \
    --distance-matrix-sample-sample ${dist_matrix} \
    --distance-matrix-sample-tree ${leaf_matrix} \
    --distance-threshold 15
    > ./nn_thresh15_$date
    """
}

process make_nearest_neighbours_20 {
    // Creates nearest neighbours with a threshold of 20

    publishDir distance_data, mode : 'move'

    input :
    file leaf_matrix from leaf_matrix_channel6
    file dist_matrix from dist_matrix_channel5
    val date

    output :
    file "nn_thresh20_${date}" into nn20_channel

    """
    python3 $distance_bin/nextflow/bin/generate_nearest_neighbours.py \
    --distance-matrix-sample-sample ${dist_matrix} \
    --distance-matrix-sample-tree ${leaf_matrix} \
    --distance-threshold 20
    > ./nn_thresh20_$date
    """
}

