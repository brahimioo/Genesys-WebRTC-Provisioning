# Genesys WebRTC op Azure Functions

Een hervatbare Python Function App die WebRTC-phones, defaultstations, skills en talen inricht. Begin met START_HIER.md.

## Onderdelen

| Onderdeel | Inhoud |
|---|---|
| Runtime | Azure Functions runtime 4, Python 3.12, Python v2-model |
| Hosting | Flex Consumption, 2.048 MB, maximale schaal één per functiegroep |
| Planning | Scan iedere 15 minuten, herstel iedere 5 minuten, extra nachtelijke scan |
| Werkverdeling | Eén gebruiker per queuebericht |
| Status | WebRtcState in Azure Table Storage |
| Doelgroep | Volledige snapshot van een expliciete Genesys-groep |
| Audit | Voor/na, gebruiker, opdrachtgeneratie en uitkomst in WebRtcAudit |
| Secrets | Genesys-clientsecret in Key Vault |
| Identiteit | Managed Identity binnen Azure; client credentials voor Genesys |
| Beheer | Entra-beveiligde HTTP-API met extra beheerderscontrole |
| Monitoring | Application Insights, Workbook en optionele alerts |
| Deployment | Cloud Shell-scripts en GitHub OIDC-workflow |

Nieuwe en onvolledige taken zijn herstelbaar. Afgeronde gebruikers worden standaard na 24 uur opnieuw gecontroleerd. Een gewijzigde configuratie krijgt een nieuwe fingerprint.

## Vereisten

- Azure-subscriptie met Flex Consumption in de gekozen regio.
- Rechten om resources aan te maken en Azure-rollen toe te kennen: bijvoorbeeld Owner, of Contributor gecombineerd met een passende RBAC-beheerrol.
- Entra-rechten om de optionele API-appregistratie aan te maken, of een vooraf aangemaakte registratie.
- Genesys client-credentialsclient met passende rollen en divisions; zie docs/GENESYS_PERMISSIONS.md.
- Werkende WebRTC-baseconfiguratie met site en line base settings.
- Expliciete doelgroepgroep en afgesproken pilotgebruikers.

De basisexport gebruikt publieke Azure-service-endpoints met TLS en identiteit. Wanneer een vaste uitgaande IP-allowlist vereist is, laat eerst de passende VNet/NAT-configuratie toevoegen.

## Configuratie

Kopieer config.example.json naar config.local.json. Git en export scripts sluiten lokale configuratiebestanden uit.

| Veld | Betekenis |
|---|---|
| subscriptionId | Azure-subscriptie |
| resourceGroupName, location | Resourcegroep en regio |
| projectName, stage | Naamgeving; stage is test of prod |
| entraApiClientId | API-appregistratie; leeg voor optionele bootstrap |
| adminObjectIds | Entra-object-IDs van beheerders; leeg gebruikt de aangemelde gebruiker |
| genesysEnvironment | Genesys-regiohost; standaard mypurecloud.de |
| genesysClientId | Genesys OAuth-client; secret wordt apart gevraagd |
| sourceGroupId | Enige toegelaten Genesys-doelgroep |
| siteId, phoneBaseSettingsId, lineBaseSettingsId | Provisioningprofiel |
| routingSkillId, routingLanguageId | Optionele toewijzingen |
| skillProficiency, languageProficiency | Waarden voor nieuwe toewijzingen |
| pilotUserIds | Extra beperking binnen de doelgroep |
| dryRun | Standaard true: voorkomt Genesys-schrijfacties |
| defaultStationPolicy | if_missing bewaart een bestaande default; enforce vervangt deze |
| preferredNamePolicy | disabled of fill_if_empty |
| givenNames | Betrouwbare mapping user-ID naar voornaam voor fill_if_empty |
| maxJobsPerScan | Standaard 50 te plannen gebruikers per scan |
| maxAttempts | Standaard acht pogingen per cyclus |
| enableAlerts, alertEmail | Na de pilot inschakelen en operationeel adres instellen |

Bestaande proficiency en bewust ingevulde preferredName blijven behouden. Een voornaam wordt nooit uit het eerste woord van een volledige naam afgeleid.

Een kleine maxJobsPerScan betekent dat de eerste uitrol meerdere scanrondes nodig kan hebben. Stem capaciteit en API-budget tijdens de pilot af.

## Installeren

Volg START_HIER.md. bootstrap.py registreert de Azure-providers, maakt resources en rollen en bewaart de Genesys-secret in Key Vault. De geheime parameter wordt via een tijdelijk bestand met beperkte rechten doorgegeven en daarna verwijderd. Niet-geheime outputs komen in deployment.local.json.

Preview op een bestaande resourcegroep:

    python3 scripts/bootstrap.py --config config.local.json --what-if

De preview maakt geen appregistratie, providerregistraties of resourcegroep. Hij gebruikt een fictieve secretwaarde; het getoonde secretdifference is daardoor geen voorstel om je echte secret te wijzigen.

De optionele Entra-bootstrap publiceert user_impersonation en preautoriseert Microsoft Azure CLI. Wanneer tenantbeleid dat blokkeert, laat een Entra-beheerder de registratie en toestemming configureren. De scripts passen geen tenantbreed toestemmingsbeleid aan.

Gebruik voor productie een aparte configuratie, resourcegroep, opslag, Genesys OAuth-client en GitHub Environment.

## Exports opnieuw bouwen

    python3 scripts/build_export.py

Dit maakt:
- dist/genesys-webrtc-azure-export.zip: volledige bronexport.
- dist/functionapp-source.zip: alleen deploybare broncode voor een remote build.
- dist/azuredeploy.parameters.example.json: niet-geheime voorbeeldparameters.

De CI compileert infra/main.bicep naar ARM JSON en levert aanvullende testresultaten, dependencyversies en checksums. De bronexport zelf bevat Bicep; een uitgevoerde compilatie is vereist om gecompileerde ARM JSON te verkrijgen.

## Beheer-API

Alle HTTP-routes vereisen App Service Authentication met Entra ID. Daarnaast controleert de code tenant en adminObjectIds of toepassingsrol Provisioning.Admin. Laat Easy Auth ingeschakeld: de principalheader is alleen betrouwbaar achter deze platformvalidatie.

| Methode | Route | Gebruik |
|---|---|---|
| GET | /api/health | Mode, pauze en configuratieversie |
| POST | /api/scan | Scan asynchroon aanvragen |
| POST | /api/provision | Eén gebruiker opnieuw aanbieden |
| GET | /api/status | Begrensd overzicht, met optionele statusfilter |
| GET | /api/status/{userId} | Voortgang van één gebruiker |
| GET | /api/audit/{userId} | Maximaal 100 auditregels in opslagvolgorde |
| PATCH | /api/control | Centrale pauze |

Voorbeelden:

    python3 scripts/api.py --outputs deployment.local.json health
    python3 scripts/api.py --outputs deployment.local.json scan
    python3 scripts/api.py --outputs deployment.local.json status --user-id <GENESYS_USER_ID>
    python3 scripts/api.py --outputs deployment.local.json provision --user-id <GENESYS_USER_ID>
    python3 scripts/api.py --outputs deployment.local.json pause
    python3 scripts/api.py --outputs deployment.local.json resume

provisioningPaused in de appconfiguratie is een strengere pauze; die moet via de configuratie worden opgeheven.

## Herstel

- Lookupfouten betekenen nooit dat een telefoon ontbreekt.
- Een opgeslagen intent gaat vooraf aan phonecreatie.
- Bij een onzekere POST-uitkomst wordt eerst opnieuw gezocht.
- Als de phone onvindbaar blijft, volgt geen automatische tweede creatie.
- Defaultstation, skills en taal worden apart gecontroleerd en na aanpassing opnieuw gelezen.
- associatedStation en effectiveStation tellen niet als bewijs van defaultStation.
- Retrybudgetten blijven over scanrondes behouden.
- Een duurzame pendingstatus blijft herstelbaar als het queuebericht niet verzonden kon worden.
- Generaties, leases en ETags beperken dubbele of concurrerende verwerking.
- Volledige groepsinventarisatie gaat vooraf aan het publiceren van een snapshot.
- Een worker gebruikt een maximaal één uur oude complete snapshot en controleert de actuele actieve gebruikersstatus. Groepswijzigingen werken na de volgende geslaagde scan door.
- Afwijkende site/base settings op bestaande phones worden gemeld; de app migreert deze niet automatisch.

Reset een onzekere creatie-intent alleen na handmatige controle dat er werkelijk geen phone is aangemaakt:

    python3 scripts/api.py --outputs deployment.local.json provision --user-id <GENESYS_USER_ID> --reset-create-intent

## CI/CD

De validatieworkflow bevat lint, pytest, JSON/workflowcontrole, Function-indexering en Bicep-compilatie. Zie VALIDATION.md voor wat daadwerkelijk uitgevoerd is.

OIDC voor code-deployment instellen:

    python3 scripts/configure_github_oidc.py --outputs deployment.local.json --repository <OWNER/REPOSITORY> --environment webrtc-test

Maak de GitHub Environment aan en voeg de vier afgedrukte Azure-variables toe. De Genesys-secret blijft in Key Vault.

De deploymentworkflow is een handmatige release via workflow_dispatch. Een nieuwe handmatige workflow moet doorgaans eerst in de standaardbranch staan om in de Actions-interface gekozen te kunnen worden. De eerste uitrol kan direct via Cloud Shell.

Flex ondersteunt geen deployment slots. Bewaar eerdere codepakketten en configuratie. Code terugzetten draait Genesys-wijzigingen niet terug.

## Validatie en grenzen

De export is geen bevestiging van een geslaagde Azure-uitrol. Voer de testset, Bicep-compilatie en pilot uit.

De app automatiseert de WebRTC-objecten uit de bestaande tool. Er is geen onbevestigde Modern WebRTC-featureflag toegevoegd. Controleer de gewenste variant en echte oproepen tijdens de pilot.

VNet/NAT, private endpoints, een eigen grafisch beheerportaal en automatische verwijdering zijn aanvullende uitbreidingen. De actuele bedrijfsstatus is beschikbaar via Table Storage en de beveiligde API; het Workbook toont gebeurtenissen en scans.

## Bronnen

- https://learn.microsoft.com/azure/azure-functions/flex-consumption-plan
- https://learn.microsoft.com/azure/azure-functions/functions-infrastructure-as-code
- https://learn.microsoft.com/azure/azure-functions/functions-how-to-github-actions
- https://learn.microsoft.com/azure/azure-functions/functions-bindings-storage-queue-trigger
- https://learn.microsoft.com/azure/app-service/overview-authentication-authorization
- https://learn.microsoft.com/azure/app-service/app-service-key-vault-references
- https://help.genesys.cloud/articles/configure-the-genesys-cloud-webrtc-phone/
- https://github.com/MyPureCloud/platform-client-sdk-python/tree/master/build/docs
