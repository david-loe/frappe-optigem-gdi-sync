-----------------------------------------------------------
-- Tabelle: Contact anlegen und mit Testdaten befüllen
-----------------------------------------------------------
IF NOT EXISTS (
  SELECT *
  FROM sys.tables
  WHERE name = 'Contact'
) BEGIN CREATE TABLE Contact (
  ContactID INT IDENTITY(1, 1) PRIMARY KEY,
  Vorname NVARCHAR(50) NOT NULL,
  Nachname NVARCHAR(50) NOT NULL,
  Email NVARCHAR(100) NOT NULL,
  Telefon NVARCHAR(20) NULL,
  Änderung DATETIME NOT NULL,
  fk NVARCHAR(100) NULL
);
END;
-- Testdaten in Contact einfügen, falls die Tabelle leer ist
IF NOT EXISTS (
  SELECT TOP 1 1
  FROM Contact
) BEGIN
INSERT INTO Contact (
    Vorname,
    Nachname,
    Email,
    Telefon,
    Änderung,
    fk
  )
VALUES (
    'Max',
    'Mustermann',
    'max.mustermann@example.com',
    '0123456789',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Erika',
    'Musterfrau',
    'erika.musterfrau@example.com',
    '0123456790',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Hans',
    'Schmidt',
    'hans.schmidt@example.com',
    '0123456791',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Anna',
    'Müller',
    'anna.mueller@example.com',
    '0123456792',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Peter',
    'Fischer',
    'peter.fischer@example.com',
    '0123456793',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Sabine',
    'Weber',
    'sabine.weber@example.com',
    '0123456794',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Klaus',
    'Wagner',
    'klaus.wagner@example.com',
    '0123456795',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Julia',
    'Becker',
    'julia.becker@example.com',
    '0123456796',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Stefan',
    'Hoffmann',
    'stefan.hoffmann@example.com',
    '0123456797',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Monika',
    'Schneider',
    'monika.schneider@example.com',
    '0123456798',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Thomas',
    'Bauer',
    'thomas.bauer@example.com',
    '0123456799',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Claudia',
    'Richter',
    'claudia.richter@example.com',
    '0123456800',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Andreas',
    'Klein',
    'andreas.klein@example.com',
    '0123456801',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Birgit',
    'Schulz',
    'birgit.schulz@example.com',
    '0123456802',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Jürgen',
    'Neumann',
    'juergen.neumann@example.com',
    '0123456803',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Martina',
    'Zimmermann',
    'martina.zimmermann@example.com',
    '0123456804',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Uwe',
    'Hartmann',
    'uwe.hartmann@example.com',
    '0123456805',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Carla',
    'Lang',
    'carla.lang@example.com',
    '0123456806',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Dieter',
    'Schmitt',
    'dieter.schmitt@example.com',
    '0123456807',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Lena',
    'Krüger',
    'lena.krueger@example.com',
    '0123456808',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Markus',
    'Meier',
    'markus.meier@example.com',
    '0123456809',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Nina',
    'Schäfer',
    'nina.schaefer@example.com',
    '0123456810',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Oliver',
    'Maier',
    'oliver.maier@example.com',
    '0123456811',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Sabina',
    'Huber',
    'sabina.huber@example.com',
    '0123456812',
    '2025-02-11 12:00:00',
    NULL
  ),
  (
    'Bernd',
    'Scholz',
    'bernd.scholz@example.com',
    '0123456813',
    '2025-02-11 12:00:00',
    NULL
  );
END;