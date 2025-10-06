## Key .env knobs

WS_COMMITMENT=processed|confirmed|finalized  
RPC_COMMITMENT=processed|confirmed|finalized   # defaults to WS_COMMITMENT  
USE_LOGS_MENTIONS=true/false  
USE_PROGRAM_SUBSCRIBE=true/false  
USE_LOGS_ALL=true/false  # debug firehose  

RATE_LIMIT_RPS, RATE_LIMIT_BURST, CIRCUIT_429_THRESHOLD, CIRCUIT_OPEN_MS tune resilience under 429s.
OWNER_WHITELIST can be empty; if empty, an effective whitelist of PROGRAM_IDS + Tokenkeg is used.
