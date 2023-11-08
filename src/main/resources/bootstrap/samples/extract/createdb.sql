CREATE TABLE speakers (
                          speaker_id INT NOT NULL PRIMARY KEY,
                          title VARCHAR(50) NOT NULL,
                          author VARCHAR(20) NOT NULL,
                          submission_date TIMESTAMP,
                          last_update_at TIMESTAMP
);

CREATE TABLE votes (
                       vote_id INT NOT NULL PRIMARY KEY,
                       speaker_id INT NOT NULL,
                       rating INT NOT NULL,
                       vote_date TIMESTAMP,
                       last_update_at TIMESTAMP,
                       FOREIGN KEY (speaker_id) REFERENCES speakers(speaker_id)
);


INSERT INTO speakers VALUES (1, 'speaker1', 'title1', '2020-06-22 19:10:25', current_timestamp);
INSERT INTO votes VALUES (1, 1, 3, '2022-01-14', '2022-01-15');
INSERT INTO votes VALUES (2, 1, 4, '2022-01-15', '2022-01-15');
INSERT INTO votes VALUES (3, 1, 5, '2022-01-15', '2022-01-15');

INSERT INTO speakers VALUES (2, 'speaker2', 'title2', '2021-01-15 16:00:00', current_timestamp);
INSERT INTO votes VALUES (4, 2, 4, '2022-01-14', '2022-01-15');
INSERT INTO votes VALUES (5, 2, 4, '2022-01-15', '2022-01-15');
INSERT INTO votes VALUES (6, 2, 3, '2022-01-15', '2022-01-15');
