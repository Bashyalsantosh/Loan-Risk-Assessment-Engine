CREATE TABLE credit_profiles (
    profile_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    credit_score INT NOT NULL,
    risk_category VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE loan_applications (
    application_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    profile_id UUID REFERENCES credit_profiles(profile_id) ON DELETE CASCADE,
    requested_amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
