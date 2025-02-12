-----------------------------------------------------------
-- Tabelle: Land anlegen und mit Testdaten befüllen
-----------------------------------------------------------
IF NOT EXISTS (
    SELECT *
    FROM sys.tables
    WHERE name = 'Land'
) BEGIN CREATE TABLE Land (
    LandID INT IDENTITY(1, 1) PRIMARY KEY,
    Name NVARCHAR(50) NOT NULL,
    ISO_Code NVARCHAR(3) NOT NULL,
    Hauptstadt NVARCHAR(50) NOT NULL
);
END;
-- Testdaten in Land einfügen, falls die Tabelle leer ist
IF NOT EXISTS (
    SELECT TOP 1 1
    FROM Land
) BEGIN
INSERT INTO Land (Name, ISO_Code, Hauptstadt)
VALUES ('Deutschland', 'DE', 'Berlin'),
    ('Frankreich', 'FR', 'Paris'),
    ('Italien', 'IT', 'Rom'),
    ('Spanien', 'ES', 'Madrid'),
    ('Vereinigtes Königreich', 'UK', 'London'),
    ('Niederlande', 'NL', 'Amsterdam'),
    ('Belgien', 'BE', 'Brüssel'),
    ('Schweiz', 'CH', 'Bern'),
    ('Österreich', 'AT', 'Wien'),
    ('Schweden', 'SE', 'Stockholm'),
    ('Norwegen', 'NO', 'Oslo'),
    ('Dänemark', 'DK', 'Kopenhagen'),
    ('Finnland', 'FI', 'Helsinki'),
    ('Portugal', 'PT', 'Lissabon'),
    ('Irland', 'IE', 'Dublin'),
    ('Polen', 'PL', 'Warschau'),
    ('Tschechien', 'CZ', 'Prag'),
    ('Ungarn', 'HU', 'Budapest'),
    ('Griechenland', 'GR', 'Athen'),
    ('Russland', 'RU', 'Moskau'),
    ('China', 'CN', 'Peking'),
    ('Japan', 'JP', 'Tokio'),
    ('Indien', 'IN', 'Neu-Delhi'),
    ('Brasilien', 'BR', 'Brasília'),
    ('USA', 'US', 'Washington, D.C.');
END;