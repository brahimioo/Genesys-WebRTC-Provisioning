# Genesys-rechten en voorwaarden

Controleer de actuele API Explorer voor de eigen organisatie. Ken de client een eigen rol en de benodigde divisions toe.

| Handeling | Gedocumenteerde rechten |
|---|---|
| Gebruiker lezen | Controleer directory:user:view in UsersApi |
| Groepsleden lezen | SDK vermeldt geen vereiste permission |
| Stations lezen | SDK vermeldt geen vereiste permission |
| Phones lezen en maken | telephony:plugin:all |
| Site/base settings/line base settings | telephony:plugin:all; site-view kan apart beschikbaar zijn |
| GET /users/{id}/station | telephony:otherStationAssociation:view |
| Defaultstation toewijzen | telephony:plugin:all OF telephony:phone:assign |
| Routing skill toewijzen | routing:skill:assign |
| Routingtaal toewijzen | routing:skill:assign OF routing:language:assign |
| Optionele preferredName aanpassen | directory:user:edit of een andere toegestane UsersApi-beheerpermission |

Controleer aanvullende leesrechten voor routinglijsten en het optionele discoveryscript, bijvoorbeeld groepen en skills. De doelgebruiker heeft volgens de defaultphone-handleiding conversation:call:add nodig. Controleer ook de telefonielicentie.

Voorwaarden:
- Koppel OAuth-rollen aan de juiste divisions.
- Eén WebRTC-phone per gebruiker.
- Phone-ID en station-ID zijn verschillende objecten.
- Alleen defaultStation bewijst de default; associatedStation en effectiveStation niet.
- De phonequery gebruikt webRtcUser.id; de stationquery gebruikt webRtcUserId.
- Azure Managed Identity vervangt de Genesys client-credentialsaanmelding niet.
- Bij een Genesys IP-allowlist is een passende vaste uitgaande route nodig.

Bronnen:
- https://github.com/MyPureCloud/platform-client-sdk-python/blob/master/build/docs/UsersApi.md
- https://github.com/MyPureCloud/platform-client-sdk-python/blob/master/build/docs/TelephonyProvidersEdgeApi.md
- https://github.com/MyPureCloud/platform-client-sdk-python/blob/master/build/docs/GroupsApi.md
- https://github.com/MyPureCloud/platform-client-sdk-python/blob/master/build/docs/StationsApi.md
- https://github.com/MyPureCloud/platform-client-sdk-python/blob/master/build/PureCloudPlatformClientV2/models/user_stations.py
- https://help.genesys.cloud/articles/assign-default-phone/
