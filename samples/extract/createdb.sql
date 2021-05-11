CREATE TABLE speakers (
                               id INT NOT NULL,
                               title VARCHAR(50) NOT NULL,
                               author VARCHAR(20) NOT NULL,
                               submission_date DATE
);

CREATE TABLE votes (
                          id INT NOT NULL,
                          speaker_id INT NOT NULL,
                          rating INT NOT NULL,
                          vote_date DATE
);

