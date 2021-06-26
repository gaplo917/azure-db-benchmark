class WriteQueries {
  static insertCompanySQL = `
    INSERT INTO companies(
      id,
      name,
      image_url,
      created_at,
      updated_at
    ) VALUES (
      nextval('companies_id_seq'),
      $1,
      $2,
      $3,
      $4
    ) RETURNING id;
  `
  static companyToQueryParam = company => {
    return [company.name, company.image_url, company.created_at, company.updated_at]
  }

  static insertCampaignSQL = `
    INSERT INTO campaigns(
      id,
      company_id,
      name,
      cost_model,
      state,
      monthly_budget,
      blacklisted_site_urls,
      created_at,
      updated_at
    ) VALUES (
      nextval('campaigns_id_seq'),
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      $7,
      $8
    ) RETURNING id;
  `
  static campaignToQueryParam = ({ companyId, campaign }) => {
    return [
      companyId,
      campaign.name,
      campaign.cost_model,
      campaign.state,
      campaign.monthly_budget,
      campaign.blacklisted_site_urls,
      campaign.created_at,
      campaign.updated_at
    ]
  }
  static insertAdSQL = `
    INSERT INTO ads(
      id,
      company_id,
      campaign_id,
      name,
      image_url,
      target_url,
      impressions_count,
      clicks_count,
      created_at,
      updated_at
    ) VALUES (
      nextval('ads_id_seq'),
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      $7,
      $8,
      $9
    ) RETURNING id;
  `

  static adToQueryParam = ({ companyId, campaignId, ad }) => {
    return [
      companyId,
      campaignId,
      ad.name,
      ad.image_url,
      ad.target_url,
      ad.impressions_count,
      ad.clicks_count,
      ad.created_at,
      ad.updated_at
    ]
  }
  static insertClicksSQL = `
    INSERT INTO clicks(
      id,
      company_id,
      ad_id,
      clicked_at,
      site_url,
      cost_per_click_usd,
      user_ip,
      user_data
    ) VALUES (
      nextval('clicks_id_seq'),
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      $7
    ) RETURNING id;
  `
  static clickToQueryParam = ({ companyId, adId, click }) => {
    return [
      companyId,
      adId,
      click.clicked_at,
      click.site_url,
      click.cost_per_click_usd,
      click.user_ip,
      click.user_data
    ]
  }

  static insertImpressionSQL = `
    INSERT INTO impressions(
      id,
      company_id,
      ad_id,
      seen_at,
      site_url,
      cost_per_impression_usd,
      user_ip,
      user_data
    ) VALUES (
      nextval('impressions_id_seq'),
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      $7
    ) RETURNING id;
  `
  static impressionToQueryParam = ({ companyId, adId, impression }) => {
    return [
      companyId,
      adId,
      impression.seen_at,
      impression.site_url,
      impression.cost_per_impression_usd,
      impression.user_ip,
      impression.user_data
    ]
  }
}

module.exports = { WriteQueries }
