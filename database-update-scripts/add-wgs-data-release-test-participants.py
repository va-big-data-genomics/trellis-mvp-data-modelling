#!/usr/bin/env python3

import pdb
import argparse
from os import listdir
from os.path import isfile, join

from neo4j import GraphDatabase

# CSV format:
# ,shipping_id
def get_csv_data(samples_csv):
    
    data = []

    with open(samples_csv, 'r') as fh:
        # fh.readline()   # Skip the first line
        for line in fh:
            bare_line = line.rstrip()
            data.append({
                         "shippingId": bare_line,
            })

    return data

def add_participants_to_study(driver, data, dry_run=False):
    # Participant nodes are designed to have study-specific data for that person
    cypher = """
        WITH $data AS samples
        UNWIND samples AS sample
        MATCH (study:Study {name:"WgsDataReleaseTestSmall"}), 
              (s:Sample)<-[:GENERATED]-(p:Person)
        WHERE s.sample = sample.shippingId
        MERGE (study)-[:HAS_PARTICIPANT]->(par:Participant {studyName: "WgsDataReleaseTestSmall", sample: sample.shippingId})-[:IS]->(p)
        RETURN p
    """

    if dry_run:
        data = data[:10]
    
    pdb.set_trace()
    print("Validate the data: len(data) == 10 for test runs")

    with driver.session() as session:
        results = session.run(cypher, data=data).values()
    return(results)
    pdb.set_trace()
    print("Get results: 'results'")

def main(args):

    dry_run = args.dry_run
    uri = args.uri
    password = args.password
    samples_csv = args.csv

    pdb.set_trace()
    print("Importing data from text file.")
    data = get_csv_data(samples_csv)

    pdb.set_trace()
    print("Create database driver")
    driver = GraphDatabase.driver(uri, auth=("neo4j", password))

    pdb.set_trace()
    print("Adding participants to study")
    results = None
    results = add_participants_to_study(driver, data, dry_run)

    if results:
        print(len(results))
    else:
        print("No results.")
    print("End of main function.")
    pdb.set_trace()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

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
    parser.add_argument(
                        "-c",
                        "--csv",
                        action="store",
                        dest="csv",
                        required=True)
    parser.add_argument(
                        "-d",
                        "--dry-run",
                        action="store_true",
                        dest="dry_run",
                        default=False,
                        required=False)

    args = parser.parse_args()
    main(args)
