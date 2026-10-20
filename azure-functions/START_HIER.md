# Start hier

Dit pakket bouwt de Azure-omgeving voor Genesys WebRTC-provisioning. De voorbeeldconfiguratie start in dry-run: de app leest Genesys en bewaart een wijzigingsplan in Azure.

## 1. Open Azure Cloud Shell

Open https://shell.azure.com en kies Bash. Je hebt geen lokale administratorrechten nodig.

Download en pak de volledige export uit. Open daarna de map azure-functions. Of haal de exportbranch op:

    curl -L -o export.zip https://github.com/brahimioo/Genesys-WebRTC-Provisioning/archive/refs/heads/azure-functions-export-20261020.zip
    unzip export.zip
    cd Genesys-WebRTC-Provisioning-azure-functions-export-20261020/azure-functions
    cp config.example.json config.local.json

## 2. Configuratie invullen

Bewerk config.local.json in de Cloud Shell-editor. Vul subscriptionId en genesysClientId in. Maak een expliciete Genesys-doelgroepgroep, bijvoorbeeld AUTO_WebRTC_Provisioning. Gebruik bij voorkeur een testorganisatie.

De secret hoort niet in config.local.json. Scripts vragen deze verborgen.

## 3. IDs uit een bestaande phone ophalen

Gebruik de exacte namen uit jouw organisatie:

    python3 scripts/discover_ids.py --config config.local.json --template-phone "WebRTC - Genesys Test User 1" --group "AUTO_WebRTC_Provisioning" --skill "_Voice" --language "Nederlands"

Dit script leest alleen Genesys. Het schrijft de gevonden IDs naar het lokale configuratiebestand. Je kunt deze IDs ook handmatig invullen.

## 4. Azure-resources maken

    python3 scripts/bootstrap.py --config config.local.json --create-entra-app

Hiervoor zijn Azure-resource- en roltoekenningsrechten nodig. De optie --create-entra-app maakt alleen een API-appregistratie wanneer entraApiClientId leeg is. Een vooraf aangemaakte appregistratie mag ook.

## 5. Code deployen

    python3 scripts/deploy_code.py --outputs deployment.local.json

Deze opdracht gebruikt een remote build voor de Python-afhankelijkheden.

## 6. Dry-run controleren

Wacht op de eerste scan of vraag deze aan:

    python3 scripts/api.py --outputs deployment.local.json scan
    python3 scripts/api.py --outputs deployment.local.json status

Bekijk ook het Azure Workbook dat door de infrastructuur is aangemaakt.

## 7. Pilot uitvoeren

Vul pilotUserIds met de afgesproken pilotgebruikers. Zet dryRun op false en voer bootstrap.py en deploy_code.py opnieuw uit. Een nieuwe configuratieversie zorgt dat eerdere dry-runresultaten opnieuw beoordeeld worden.

Volg docs/PILOT.md. Schakel op het overstapmoment de bestaande GitHub-workflow Genesys WebRTC job uit.

Lees VALIDATION.md: Python-tests, Bicep-compilatie en live tenantvalidatie moeten nog uitgevoerd worden.
