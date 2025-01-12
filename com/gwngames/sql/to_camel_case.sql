CREATE OR REPLACE FUNCTION to_camel_case(input_string TEXT) RETURNS TEXT AS $$
DECLARE
    cleaned_string TEXT;
BEGIN
    -- Clean the input string and ensure apostrophes are preserved correctly
    cleaned_string := regexp_replace(input_string, '\s+', ' ', 'g');

    RETURN array_to_string(
        array(SELECT concat(upper(substr(word, 1, 1)), lower(substr(word, 2)))
              FROM regexp_split_to_table(cleaned_string, ' ') AS word),
        ' '
    );
END;
$$ LANGUAGE plpgsql;

