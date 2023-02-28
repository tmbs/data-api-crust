CREATE TABLE reference (
    s SERIAL,
    b BIT,
    t TINYINT,
    tu TINYINT UNSIGNED,
    bo BOOLEAN,
    sm SMALLINT,
    smu SMALLINT UNSIGNED,
    m MEDIUMINT,
    mu MEDIUMINT UNSIGNED,
    i INT,
    iu INT UNSIGNED,
    bi BIGINT,
    biu BIGINT UNSIGNED,
    d DECIMAL,
    du DECIMAL UNSIGNED,
    f FLOAT,
    fu FLOAT UNSIGNED,
    do DOUBLE,
    dou DOUBLE UNSIGNED,
    da DATE,
    dat DATETIME,
    ti TIMESTAMP,
    tim TIME,
    y YEAR,
    v VARCHAR(32),
    te TEXT,
    e ENUM('A', 'B'),
    j JSON
);

INSERT INTO REFERENCE VALUES (
    1,
    2,
    -3,
    4,
    false,
    -5,
    6,
    -7,
    7,
    -8,
    8,
    -9,
    9,
    -0.1,
    0.1,
    -0.2,
    0.2,
    -0.3,
    0.3,
    DATE(NOW()),
    NOW(),
    NOW(),
    TIME(NOW()),
    YEAR(NOW()),
    'foo',
    'bar',
    'A',
    JSON_OBJECT('foo', JSON_OBJECT('bar', 'baz'))
  );