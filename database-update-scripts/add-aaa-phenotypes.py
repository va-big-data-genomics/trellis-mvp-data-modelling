#!/usr/bin/env python3

import pdb
import argparse
from os import listdir
from os.path import isfile, join

from neo4j import GraphDatabase

## Add nodes
cypher_add_aaa_node = """
MERGE (n:Concept {
    conceptCode: 233985008,
    conceptId: 198177,
    conceptName: "Abdominal aortic aneurysm",
    standardConcept: True,
    validEndDate: date("2002-01-31"),
    validStartDate: date("2099-12-31"),
    dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"
})
RETURN n
"""

cypher_add_condition_node = """
MERGE (n:Domain {
    domainId: "",
    domainName: "Condition",
    dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"
})
RETURN n
"""

cypher_add_clinical_finding_node = """
MERGE (n:ConceptClass {
    conceptClassId: "",
    conceptClassName: "Clinical Finding",
    dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"
})
RETURN n
"""

cypher_add_snomed_node = """
MERGE (n:Vocabulary {
    vocabularyId: "",
    vocabularyName: "SNOMED",
    vocabularyReference: "",
    vocabularyVersion: "",
    dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"
})
RETURN n
"""

cypher_relate_aaa_condition = """
    MATCH (c:Concept), (d:Domain)
    WHERE c.conceptName = "Abdominal aortic aneurysm"
    AND d.domainName = "Condition"
    MERGE (d)-[:HAS_CONCEPT {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(c)
    MERGE (c)-[:IN_DOMAIN {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(d)
    RETURN c,d
"""

cypher_relate_aaa_clinical = """
    MATCH (c:Concept), (class:ConceptClass)
    WHERE c.conceptName = "Abdominal aortic aneurysm"
    AND class.conceptClassName = "Clinical Finding"
    MERGE (class)-[:HAS_CONCEPT {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(c)
    MERGE (c)-[:BELONGS_TO_CLASS {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(class)
    RETURN c, class
"""

cypher_relate_aaa_snomed = """
    MATCH (c:Concept), (v:Vocabulary)
    WHERE c.conceptName = "Abdominal aortic aneurysm"
    AND v.vocabularyName = "SNOMED"
    MERGE (v)-[:HAS_CONCEPT {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(c)
    MERGE (c)-[:USES_VOCABULARY {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(v)
    RETURN c, v
"""

cypher_add_aaa_phenotype_metadata = """
    WITH $data as aaaPhenotypes
    UNWIND aaaPhenotypes AS aaa
    MATCH (person:Person)-[:GENERATED]->(sample:Sample)
    WHERE sample.sample = aaa.shippingId
    SET person.aaaDiagnosis = apoc.convert.toBoolean(aaa.aaaDiagnosis),
        person.hareEthnicity = aaa.hareEthnicity,
        person.vaReportedGender = aaa.gender,
        person.ldlMeasuredDaysFromEnrollment = toInteger(aaa.ldlMeasuredDaysFromEnrollment),
        person.ldlValueNearEnrollment = apoc.convert.toFloat(aaa.ldlValueNearEnrollment),
        person.statinsGivenOnEnrollmentDate = apoc.convert.toBoolean(aaa.statinsGivenOnEnrollmentDate),
        person.ageAtEnrollment = toInteger(aaa.ageAtEnrollment)
    RETURN id(person)
"""

cypher_merge_condition_occurrence = """
    MATCH (p:Person), (c:Concept)
    WHERE p.aaaDiagnosis = True
    AND c.conceptName = "Abdominal aortic aneurysm"
    MERGE (p)-[:HAS_CONDITION_OCCURRENCE {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(:ConditionOccurrence {conditionStartDaysFromEnrollment: p.ldlMeasuredDaysFromEnrollment, dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"})-[:HAS_CONCEPT {dataModel: "https://github.com/NUSCRIPT/OMOP_to_Graph"}]->(c)
    RETURN p
"""

# Expect 0 results
cypher_validate_aaa_0 = """
    MATCH (p:Person)-[]->(:ConditionOccurrence)-[]->(:Concept {conceptName: "Abdominal aortic aneurysm"}) 
    WHERE NOT p.aaaDiagnosis = True
    RETURN p
"""

# Expect 0 results
cypher_validate_aaa_1 = """
    MATCH (p:Person)
    WHERE NOT (p)-[]->(:ConditionOccurrence)-[]->(:Concept {conceptName: "Abdominal aortic aneurysm"})
    AND p.aaaDiagnosis = True
    RETURN p
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
            elements = line.rstrip().split('|')
            data.append({
                         "shippingId": elements[0],
                         "aaaDiagnosis": yn_to_tf[elements[1]],
                         "hareEthnicity": elements[2],
                         "gender": elements[3],
                         "ldlMeasuredDaysFromEnrollment": elements[4],
                         "ldlValueNearEnrollment": elements[5],
                         "statinsGivenOnEnrollmentDate": yn_to_tf[elements[6]],
                         "ageAtEnrollment": elements[7]
            })

    return data

def run_cypher_data_command(driver, data, cypher, dry_run=False):
    with driver.session() as session:
        results = session.run(cypher, data=data).values()
    return results

def run_cypher_command(driver, cypher):
    with driver.session() as session:
        results = session.run(cypher).values()
    return results

def main(args):

    dry_run = args.dry_run
    uri = args.uri
    password = args.password
    samples_csv = args.csv

    pdb.set_trace()
    print("Import CSV data into: 'data'.")
    data = get_csv_data(samples_csv)

    # Check len(results) and some results

    pdb.set_trace()
    print("Create driver: 'driver'.")
    driver = GraphDatabase.driver(uri, auth=("neo4j", password))

    pdb.set_trace()
    print("Add standardized vocabulary nodes")
    print("Adding AAA node: 'aaa_results'.")
    aaa_results = run_cypher_command(
                    driver = driver,
                    cypher = cypher_add_aaa_node)
    print("Adding Condition node: 'condition_results'.")
    condition_results = run_cypher_command(
                            driver = driver,
                            cypher = cypher_add_condition_node)
    print("Adding Clinical Findings node: 'findings_results'.")
    findings_results = run_cypher_command(
                            driver = driver,
                            cypher = cypher_add_clinical_finding_node)
    print("Adding SNOMED node: 'snomed_results'.")
    snomed_results = run_cypher_command(
                                driver = driver,
                                cypher = cypher_add_snomed_node)
    pdb.set_trace()
    print("Adding OMOP relationships")

    # Link standardized vocabulary nodes
    aaa_condition_results = run_cypher_command(
                                driver = driver,
                                cypher = cypher_relate_aaa_condition)
    aaa_clinical_results = run_cypher_command(
                            driver = driver,
                            cypher = cypher_relate_aaa_clinical)
    aaa_snomed_results = run_cypher_command(
                            driver = driver,
                            cypher = cypher_relate_aaa_snomed)

    if dry_run:
        pdb.set_trace()
        print("Dry run: only using small slice of data: 'data'.")
        data = data[0:65]
        print(f"Data: {data}.")
   
    pdb.set_trace()
    print("Adding AAA phenotype data to Person nodes: 'aaa_phenotype_results'.")
    aaa_phenotype_results = run_cypher_data_command(
                                driver = driver,
                                data = data,
                                cypher = cypher_add_aaa_phenotype_metadata)
    if aaa_phenotype_results:
        print(len(aaa_phenotype_results))
    else:
        print("No results.")

    pdb.set_trace()
    print("Create ConditionOccurrence node for Persons with AAA")
    occurrence_results = run_cypher_command(
                            driver = driver,
                            cypher = cypher_merge_condition_occurrence)
    if occurrence_results:
        print(len(occurrence_results))
    else:
        print("No ConditionOccurrence results.")

    pdb.set_trace()
    print("Validate AAA phenotype: ['validation_results_0', 'validation_results_1']")
    validation_results_0 = run_cypher_command(
                            driver = driver,
                            cypher = cypher_validate_aaa_0)
    print(f"Validation result 0: {len(validation_results_0) == 0}")

    validation_results_1 = run_cypher_command(
                            driver = driver,
                            cypher = cypher_validate_aaa_1)
    print(f"Validation result 1: {len(validation_results_1) == 0}")

    pdb.set_trace()
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
