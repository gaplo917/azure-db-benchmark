-- drop all tables
DROP TABLE IF EXISTS impressions;
DROP TABLE IF EXISTS clicks;
DROP TABLE IF EXISTS ads;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS companies;

CREATE TABLE companies (
  id bigserial PRIMARY KEY,
  name text NOT NULL,
  image_url text,
  created_at timestamp without time zone NOT NULL,
  updated_at timestamp without time zone NOT NULL
);

CREATE INDEX companies_created_at_idx ON companies (created_at);
CREATE INDEX companies_updated_at_idx ON companies (updated_at);

CREATE TABLE campaigns (
  id bigserial,
  company_id bigint REFERENCES companies (id),
  name text NOT NULL,
  cost_model text NOT NULL,
  state text NOT NULL,
  monthly_budget bigint,
  blacklisted_site_urls text[],
  created_at timestamp without time zone NOT NULL,
  updated_at timestamp without time zone NOT NULL,

  PRIMARY KEY (company_id, id)
);

CREATE INDEX campaigns_created_at_idx ON campaigns (created_at);
CREATE INDEX campaigns_updated_at_idx ON campaigns (updated_at);
CREATE INDEX campaigns_state_idx ON campaigns (state);
CREATE INDEX campaigns_monthly_budget_idx ON campaigns (monthly_budget);

CREATE TABLE ads (
    id bigserial,
    company_id bigint,
    campaign_id bigint,
    name text NOT NULL,
    image_url text,
    target_url text,
    impressions_count bigint DEFAULT 0,
    clicks_count bigint DEFAULT 0,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,

    PRIMARY KEY (company_id, id),
    FOREIGN KEY (company_id, campaign_id)
        REFERENCES campaigns (company_id, id)
);

CREATE INDEX ads_created_at_idx ON ads (created_at);
CREATE INDEX ads_updated_at_idx ON ads (updated_at);

CREATE TABLE clicks (
    id bigserial,
    company_id bigint,
    ad_id bigint,
    clicked_at timestamp without time zone NOT NULL,
    site_url text NOT NULL,
    cost_per_click_usd numeric(20,10),
    user_ip inet NOT NULL,
    user_data jsonb NOT NULL,

    PRIMARY KEY (company_id, id),
    FOREIGN KEY (company_id, ad_id)
        REFERENCES ads (company_id, id)
);

CREATE INDEX clicks_cost_per_click_usd_idx ON clicks (cost_per_click_usd);
CREATE INDEX clicks_clicked_at_idx ON clicks (clicked_at);

CREATE TABLE impressions (
    id bigserial,
    company_id bigint,
    ad_id bigint,
    seen_at timestamp without time zone NOT NULL,
    site_url text NOT NULL,
    cost_per_impression_usd numeric(20,10),
    user_ip inet NOT NULL,
    user_data jsonb NOT NULL,

    PRIMARY KEY (company_id, id),
    FOREIGN KEY (company_id, ad_id)
        REFERENCES ads (company_id, id)
);

CREATE INDEX impressions_cost_per_impression_usd_idx ON impressions (cost_per_impression_usd);
CREATE INDEX impressions_seen_at_idx ON impressions (seen_at);
