import logging
import time
from neo4j import GraphDatabase


def create_fulltext_index(uri, user, password, database):
    start_time = time.time()
    logging.info("Starting the process of creating a full-text index.")

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password), database=database)
    except Exception as e:
        logging.error(f"Failed to create a database driver: {str(e)}")
        return

    try:
        with driver.session() as session:
            try:
                start_step = time.time()
                session.run("DROP INDEX entities IF EXISTS;")
                logging.info("Dropped existing index (if any) in %.2f seconds.", time.time() - start_step)
                
            except Exception as e:
                logging.error(f"Exception during drop index: {str(e)}")
                return
            
            try:
                start_step = time.time()
                result = session.run("CALL db.labels()")
                labels = [record["label"] for record in result]
                labels_str = ":" + "|".join([f"`{label}`" for label in labels])
                logging.info("Fetched labels in %.2f seconds.", time.time() - start_step)

            except Exception as e:
                logging.error(f"Exception while fetching labels: {str(e)}")
                return

            try:
                start_step = time.time()
                query = f"CREATE FULLTEXT INDEX entities FOR (n{labels_str}) ON EACH [n.id, n.description];"
                session.run(query)
                logging.info("Created full-text index in %.2f seconds.", time.time() - start_step)

            except Exception as e:
                logging.error(f"Failed to create full-text index: {str(e)}")
                return
    except Exception as e:
        logging.error(f"An error occurred during the session: {str(e)}")
    finally:
        driver.close()
        logging.info("Driver closed.")
        logging.info("FULL TEXT INDEX created in %.2f seconds.", time.time() - start_time)