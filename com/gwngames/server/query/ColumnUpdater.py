# Function to update the authors column
from psycopg.rows import tuple_row
from psycopg_pool import AsyncConnectionPool

import app


def update_authors_column(pool: AsyncConnectionPool):
    app.logger.info("Starting the update process for Publication's authors column...")
    try:
        with pool.connection() as conn:
            fetch_query = """
                SELECT 
                    pa.publication_id AS id,
                    STRING_AGG(
                        DISTINCT to_camel_case(LOWER(a.name)),
                        ', '
                    ) AS authors
                FROM publication_author pa
                JOIN author a 
                    ON a.id = pa.author_id
                GROUP BY pa.publication_id;
            """

            with conn.cursor(row_factory=tuple_row) as cur:
                cur.execute(fetch_query)
                results = cur.fetchall()

            update_query = """
                UPDATE publication
                SET authors = data.authors
                FROM (VALUES %s) AS data (id, authors)
                WHERE publication.id = data.id;
            """

            with conn.cursor() as cur:
                cur.executemany(
                    update_query,
                    [(row[0], row[1]) for row in results]
                )

            conn.commit()
            app.logger.info("Authors column of Publication updated successfully!")

    except Exception as e:
        app.logger.info(f"Error during update: {e}")
