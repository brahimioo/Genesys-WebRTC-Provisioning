# Beheer en herstel

## Statussen

pending is opgeslagen werk; running heeft een lease; retry heeft een volgende herprobeertijd. dry_run bevat alleen een wijzigingsplan. succeeded betekent dat de verplichte stappen zijn geverifieerd. succeeded_with_warnings meldt bijvoorbeeld een behouden bestaande default. excluded is buiten de actieve doelgroep. cancelled hoort bij een oude configuratie. blocked vraagt beheer.

Een gewone scan reset blocked niet. Een expliciete beheeropdracht of gewijzigde configuratie kan een nieuwe cyclus starten. Een onzekere creatie-intent blijft behouden totdat een phone gevonden wordt of een beheerder deze expliciet reset.

## Pauze

Gebruik scripts/api.py pause en resume. Voor iedere nieuwe wijziging wordt de centrale pauze gecontroleerd. Een reeds lopende API-aanvraag kan nog afronden.

Een lease duurt 15 minuten bij een Functions-timeout van 5 minuten. Na een crash kan herstel dus even wachten. De herstelrun controleert iedere 5 minuten.

## Monitoring

Gebeurtenissen hebben het prefix webrtc_event. Voorbeeldquery in de Log Analytics-workspace:

    AppTraces
    | where Message startswith "webrtc_event "
    | extend e = parse_json(substring(Message,13))
    | project TimeGenerated, Event=tostring(e.event), UserId=tostring(e.user_id), Code=tostring(e.code)
    | order by TimeGenerated desc

Het Workbook toont scans, fouten en achterstand. Maintenance logt backlog_snapshot met aantallen en leeftijd van wachtend werk.

De foutalert kijkt naar user_failed, scan_failed en poison_message. De heartbeatalert signaleert het uitblijven van een geslaagde scan en kan ook tijdens een bewuste pauze afgaan. Gebruik daarvoor een geplande Alert Processing Rule of schakel alerts tijdelijk uit. De voorbeeldconfiguratie heeft alerts uitgeschakeld tot de pilot afgerond is.

## Audit en retentie

Gebruik gebruiker plus generation om auditregels aan een opdracht te koppelen. Een crash kan een intent zonder afsluitende regel achterlaten; controleer de actuele Genesys-toestand en WebRtcState.

- Technische logs: 30 dagen.
- Audit: 90 dagen; begrensde opruiming door onderhoud.
- Groepssnapshots: 2 dagen.
- Gebruikersstatus blijft bestaan voor idempotentie en herstel.
- Een optionele voornaam in audit is een persoonsgegeven; stem toegang en retentie intern af.
- Status- en audit-API leveren begrensde selecties; gebruik Table Storage voor volledige exports.

## Secretrotatie

1. Pauzeer provisioning.
2. Roteer de Genesys-clientsecret.
3. Werk Key Vault-secret genesys-client-secret bij.
4. Ververs de Key Vault-reference en herstart de Function App om configuratie en geheugentokens te vernieuwen.
5. Controleer authenticatie, een scan en een pilotgebruiker.
6. Hervat.

Tokens worden vóór afloop vernieuwd; na een 401 wordt één nieuwe authenticatie geprobeerd. Tokens staan niet in de gebruikersstatus of logs.

## Terugzetten

Pauzeer, bewaar relevante audit en status, deploy het vorige codepakket met passende configuratie en herstel zo nodig individuele Genesys-instellingen. Begin daarna opnieuw met pilotgebruikers.

Een vorige codeversie draait Genesys-wijzigingen niet automatisch terug. Een algemene bulkverwijdering is geen geschikte rollback.

## Capaciteit en kosten

Meet uitvoeringsduur, transacties en logvolume tijdens de pilot. Compute, Storage, Key Vault en Log Analytics hebben afzonderlijke meters. Extra always-ready-capaciteit, Premium, NAT en private networking kunnen aanvullende vaste kosten geven.

De scanner en worker delen een rate limiter. Houd rekening met aanvullende tenant- en endpointspecifieke Genesys-limieten. Vergroot de schaal pas na belastingtests.
