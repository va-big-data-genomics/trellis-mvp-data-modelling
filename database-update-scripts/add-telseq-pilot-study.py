#!/usr/bin/env python3

import sys
import pdb
import argparse
from os import listdir
from os.path import isfile, join
from datetime import date

from neo4j import GraphDatabase

# Cypher queries

## Add nodes
cypher_create_study_node = """
    MERGE (study:Study {
        name: "WgsTelseqPilot",
        type: "Pilot",
        lead: "Prathima Vembu",
        bucketName: "",
        resultsDirectory: "Output/dsub-batch-submission/85k_samples",
        phenotypeObjectUri: "",
        aggregatedResultsObjectUri: "",
        softwareName: "telseq",
        softwareVersion: "v0.0.2",
        softwareSource: "https://github.com/zd1/telseq",
        softwareDockerfile: "https://github.com/va-big-data-genomics/dockerfiles/blob/main/telseq-0.0.2",
        softwareDockerImage: "https://hub.docker.com/r/pvembu/telseq",
        notes: "Pilot study to analyze telomere length by applying the telseq algorithm to whole genome sequencing data in CRAM format. Telseq result files were generated by manually launching dsub jobs."
        })
    RETURN study
    """

cypher_create_participant_nodes = """
    UNWIND $data as telseqResult
    MERGE (participant:Participant {
        study: $study,
        sample: telseqResult.shippingId})
    ON CREATE SET participant.telomereLengthEstimate = toFloat(telseqResult.lengthEstimate)
    RETURN participant
    """

cypher_relate_participants_to_study = """
    CALL apoc.periodic.iterate(
        "MATCH (participant:Participant), (study:Study) WHERE participant.study = $study AND study.name = $study RETURN participant, study",
        "MERGE (study)-[has:HAS_PARTICIPANT]->(participant)",
        {batchSize: 200, parallel: true, params:{study: $study}}
    )
    """

cypher_relate_participants_to_samples = """
    CALL apoc.periodic.iterate(
        "MATCH (participant:Participant {study: $study}) RETURN participant",
        "MATCH (person:Person)-[:GENERATED]->(sample:Sample) WHERE participant.sample = sample.sample MERGE (participant)-[is:IS]->(person) MERGE (participant)-[provided:PROVIDED_SAMPLE]->(sample)",
        {batchSize: 200, parallel: true, params:{study: $study}}
    )
    """

cypher_add_study_participant_count = """
    MATCH (study:Study)-[:HAS_PARTICIPANT]->(par:Participant)
    WHERE study.name = $study
    WITH study, COLLECT(par) AS participants
    SET study.participantCount = SIZE(participants),
        study.lastUpdated = date($date)
    RETURN SIZE(participants)
    """

def get_csv_data(samples_csv):
    
    data = []

    yn_to_tf = {
                "Y": True,
                "N": False
    }

    with open(samples_csv, 'r') as fh:
        fh.readline()   # Skip the first line
        for line in fh:
            elements = line.rstrip().split(',')
            data.append({
                         "shippingId": elements[0],
                         "age": elements[1],
                         "sex": elements[2],
                         "ethnicity": elements[3],
                         "aaaDiagnosis": elements[4],
                         "lengthEstimate": elements[5],
            })

    return data

def run_cypher_data_command(driver, data, cypher, dry_run=False):
    with driver.session() as session:
        results = session.run(cypher, data=data).values()
    return results

def run_cypher_command(driver, cypher, **kwargs):
    with driver.session() as session:
        results = session.run(cypher, kwargs).values()
    return results

def main(args):

    dry_run = args.dry_run
    uri = args.uri
    password = args.password
    samples_csv = args.csv

    #pdb.set_trace()
    print("Import CSV data into: 'data'.")
    data = get_csv_data(samples_csv)

    #pdb.set_trace()
    print(f"Create driver: 'driver' for uri: ${uri}.")
    driver = GraphDatabase.driver(uri, auth=("neo4j", password))

    # Study metadata
    study_name = "WgsTelseqPilot"
    study_lead = "Prathima Vembu"
    study_type = "Pilot"

    #### Create study node
    pdb.set_trace()
    print("Create study node.")
    study_node_results = run_cypher_command(
                    driver = driver,
                    cypher = cypher_create_study_node)

    #### Create participant nodes ####
    if dry_run:
        #pdb.set_trace()
        print("Dry run: only using small slice of data: 'data'.")
        data = data[0:5]
        print(f"Data: {data}.")
   
    #pdb.set_trace()
    print("Adding participant nodes: 'participant_node_results'.")
    participant_node_results = run_cypher_command(
                                driver = driver,
                                cypher = cypher_create_participant_nodes,
                                data = data,
                                study = study_name)
    if participant_node_results:
        print(len(participant_node_results))
    else:
        print("No results.")
    #sys.exit()
    ####

    #### Relate participants to study ####
    #pdb.set_trace()
    print("Relate participants to study")
    relate_participants_study_results = run_cypher_command(
        driver = driver,
        cypher = cypher_relate_participants_to_study,
        study = study_name)
    if relate_participants_study_results:
        print(relate_participants_study_results)
    else:
        print("No participants were related to study.")
    #sys.exit()
    ####

    #### Relate participants to samples ####
    #pdb.set_trace()
    print("Relate participants to samples")
    relate_participants_samples_results = run_cypher_command(
        driver = driver,
        cypher = cypher_relate_participants_to_samples,
        study = study_name)
    if relate_participants_study_results:
        print(relate_participants_samples_results)
    else:
        print("No participants were related to samples.")
    #sys.exit()
    ####

    #### Relate participants to samples ####
    #pdb.set_trace()
    print("Count study participants.")
    study_participant_count_results = run_cypher_command(
        driver = driver,
        cypher = cypher_add_study_participant_count,
        study = "WgsTelseqPilot",
        date = date.today())
    if len(study_participant_count_results):
        print(f"Count of study participants: {study_participant_count_results}.")
    else:
        print("No participants were counted.")
    ####
    print("End of script")

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser(description="Add phenotype information related to Abdominal Aortic Aneurysm to (:Person) nodes and add (:Person)-[]->(:ConditionOccurrence)-[]->(:Concept) patterns aligned with OMOP graph model.")

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("-u",
                        "--uri",
                        action="store",
                        dest="uri",
                        help="Example: 'bolt://35.199.180.180'",
                        required=True)
    parser.add_argument("-p",
                        "--password",
                        action="store",
                        dest="password",
                        required=True)
    parser.add_argument("-c",
                        "--csv",
                        action="store",
                        dest="csv",
                        help="CSV with sample information",
                        required=False)
    parser.add_argument(
                        "-d",
                        "--dry-run",
                        action="store_true",
                        dest="dry_run",
                        default=False,
                        required=False)


    args = parser.parse_args()
    main(args)
