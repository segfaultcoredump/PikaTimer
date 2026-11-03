-- PikaTimer v2.0: RunSignUp v2 API key and OAuth2 (PKCE) support
-- v2 API keys reuse the existing rsuUsername / rsuPassword columns
ALTER TABLE rsuconfig ADD COLUMN IF NOT EXISTS rsuAccessToken varchar;
ALTER TABLE rsuconfig ADD COLUMN IF NOT EXISTS rsuRefreshToken varchar;
ALTER TABLE rsuconfig ADD COLUMN IF NOT EXISTS rsuTokenExpires bigint;
ALTER TABLE rsuconfig ADD COLUMN IF NOT EXISTS rsuOAuthUser varchar;
