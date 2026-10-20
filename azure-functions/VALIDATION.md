# Validatiestatus

De bronexport bevat uitvoerbare tests, lint, Function-indexering en Bicep-compilatie via de CI-workflow.

Tijdens het samenstellen was geen lokale Python/Azure-buildruntime beschikbaar. Er was ook nog geen uitgevoerde GitHub Actions-run voor deze branch. Python-tests en Bicep-compilatie worden daarom niet als geslaagd aangemerkt.

De JSON-configuratie en de ZIP-structuur, CRC32-checksums en broninhoud van de geleverde archieven zijn gecontroleerd.

Zelf controleren vanuit azure-functions met Python 3.12:

    python -m pip install -r requirements-dev.txt
    python -m ruff check .
    python -m pytest -q
    python scripts/validate_package.py
    az bicep build --file infra/main.bicep

Een geslaagde CI-run maakt een aanvullend VALIDATION.md met commit en testresultaten in het buildartifact.

Nog in de eigen tenant uit te voeren:
- ARM-deployment, Managed Identity en IAM-propagatie.
- Entra-tokenuitgifte en autorisatie.
- Genesys-rechten, regio en provisioninggedrag.
- Een inkomende en uitgaande testoproep.
- Logging, Workbook, alerts en herstelproef.

Volg docs/PILOT.md.
