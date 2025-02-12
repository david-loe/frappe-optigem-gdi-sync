-----------------------------------------------------------
-- Tabelle: Auto anlegen und mit Testdaten befüllen
-----------------------------------------------------------
IF NOT EXISTS (
  SELECT *
  FROM sys.tables
  WHERE name = 'Auto'
) BEGIN CREATE TABLE Auto (
  AutoID INT IDENTITY(1, 1) PRIMARY KEY,
  Marke NVARCHAR(50) NOT NULL,
  Modell NVARCHAR(50) NOT NULL,
  Baujahr INT NOT NULL,
  Farbe NVARCHAR(20) NOT NULL
);
END;
-- Testdaten in Auto einfügen, falls die Tabelle leer ist
IF NOT EXISTS (
  SELECT TOP 1 1
  FROM Auto
) BEGIN
INSERT INTO Auto (Marke, Modell, Baujahr, Farbe)
VALUES ('BMW', '3er', 2018, 'Schwarz'),
  ('Audi', 'A4', 2019, 'Weiß'),
  ('Mercedes', 'C-Klasse', 2020, 'Silber'),
  ('Volkswagen', 'Golf', 2017, 'Blau'),
  ('Opel', 'Astra', 2016, 'Rot'),
  ('Ford', 'Focus', 2015, 'Grau'),
  ('Skoda', 'Octavia', 2021, 'Schwarz'),
  ('Renault', 'Clio', 2018, 'Gelb'),
  ('Peugeot', '308', 2019, 'Weiß'),
  ('Fiat', '500', 2017, 'Rot'),
  ('Nissan', 'Qashqai', 2020, 'Silber'),
  ('Toyota', 'Corolla', 2021, 'Blau'),
  ('Kia', 'Ceed', 2018, 'Weiß'),
  ('Hyundai', 'i30', 2019, 'Rot'),
  ('Volvo', 'S60', 2020, 'Grau'),
  ('Subaru', 'Impreza', 2015, 'Blau'),
  ('Mazda', 'Mazda3', 2016, 'Schwarz'),
  ('Honda', 'Civic', 2017, 'Rot'),
  ('Chevrolet', 'Cruze', 2018, 'Silber'),
  ('Citroen', 'C3', 2019, 'Weiß'),
  ('SEAT', 'Leon', 2020, 'Blau'),
  ('Lexus', 'IS', 2021, 'Schwarz'),
  ('Jaguar', 'XE', 2018, 'Rot'),
  ('Infiniti', 'Q50', 2019, 'Silber'),
  ('Mitsubishi', 'Lancer', 2017, 'Grau');
END;