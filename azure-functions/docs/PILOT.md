# Pilotchecklist

Gebruik bij voorkeur een Genesys-testorganisatie. Beperk bij productie sourceGroupId en pilotUserIds tot de afgesproken personen.

## Voorbereiden

- Controleer OAuth-rechten en divisions.
- Controleer site, phone base settings en line base settings tegen een werkende WebRTC-phone.
- Controleer telefonielicentie en conversation:call:add van de doelgebruikers.
- Controleer Entra-tokenuitgifte en beheerdersrechten.
- Test dat anonieme requests 401 krijgen en een niet-beheerder geweigerd wordt.
- Start met dryRun true.
- Voorkom gelijktijdige schrijfacties van de oude job en Azure op dezelfde gebruikers.

## Dry-run

- Een gebruiker zonder phone levert het verwachte aanmaakplan.
- Een bestaande phone met ontbrekende taal levert alleen die ontbrekende stap.
- Een afwijkende default wordt bij if_missing behouden en gemeld.
- Er zijn geen Genesys-schrijfacties.
- Het Workbook ontvangt scan_completed en user_completed.

## Live pilot

- Controleer phone, station, defaultStation, skill en taal.
- Bied dezelfde gebruiker opnieuw aan: geen dubbele phone of onnodige wijzigingen.
- Controleer herstel bij een bestaande phone met ontbrekende skill/taal.
- Controleer behoud van proficiency en preferredName.
- Controleer dat een gedeactiveerde of uitgesloten gebruiker wordt overgeslagen.
- Pauzeer en hervat; pendingwerk moet herstelbaar blijven.
- Test een herstart tussen twee stappen in de testomgeving.
- Test een inkomende en uitgaande oproep.
- Controleer audit, logging en operationele meldingen.

## Overstappen

1. Leg pilotresultaten vast.
2. Schakel de oude GitHub-workflow Genesys WebRTC job uit.
3. Activeer Azure voor de afgesproken doelgroep.
4. Pas maxJobsPerScan aan na controle van API-budget en doorlooptijd.
5. Activeer alerts.
6. Controleer de eerste volledige scan en nachtelijke hercontrole.
7. Verwijder de oude Genesys-secret uit GitHub zodra Azure bevestigd werkt.
