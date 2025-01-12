CREATE INDEX idx_publication_author_pub_id ON publication_author (publication_id);
CREATE INDEX idx_publication_author_author_id ON publication_author (author_id);
CREATE INDEX idx_publication_title ON publication (title);
CREATE INDEX idx_author_name ON author (name);
