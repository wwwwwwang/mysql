BEGIN
	DECLARE t_error INTEGER DEFAULT 0;
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
	
	START TRANSACTION;

	#campaign
	INSERT INTO `tvl_report_campaign` (`timestamp`, `project_id`, `campaign_id`, `material_id`, `bids`
		, `wins`, `imps`, `clks`, `vimps`, `vclks`, `cost`, `create_timestamp`)
	SELECT `timestamp`, `project_id`, `campaign_id`, `material_id`, `bids`
		, `wins`, `imps`, `clks`, `vimps`, `vclks`, `cost`, unix_timestamp() AS `create_timestamp`
	FROM tvl_report_campaign_mem
	ON DUPLICATE KEY UPDATE tvl_report_campaign.bids = tvl_report_campaign.bids + VALUES(bids)
	, tvl_report_campaign.wins = tvl_report_campaign.wins + VALUES(wins)
	, tvl_report_campaign.imps = tvl_report_campaign.imps + VALUES(imps)
	, tvl_report_campaign.clks = tvl_report_campaign.clks + VALUES(clks)
	, tvl_report_campaign.vimps = tvl_report_campaign.vimps + VALUES(vimps)
	, tvl_report_campaign.vclks = tvl_report_campaign.vclks + VALUES(vclks)
	, tvl_report_campaign.cost = tvl_report_campaign.cost + VALUES(cost)
	, `update_timestamp` = VALUES(`create_timestamp`);
	
	#campaign with location
	INSERT INTO `tvl_report_campaign_location` (`timestamp`, `project_id`, `campaign_id`, `material_id`, `bids`
		, `location`, `wins`, `imps`, `clks`, `vimps`, `vclks`, `cost`, `create_timestamp`)
	SELECT `timestamp`, `project_id`, `campaign_id`, `material_id`, `bids`
		, `location`, `wins`, `imps`, `clks`, `vimps`, `vclks`, `cost`, unix_timestamp() AS `create_timestamp`
	FROM tvl_report_campaign_location_mem
	ON DUPLICATE KEY UPDATE tvl_report_campaign_location.bids = tvl_report_campaign_location.bids + VALUES(bids)
	, tvl_report_campaign_location.wins = tvl_report_campaign_location.wins + VALUES(wins)
	, tvl_report_campaign_location.imps = tvl_report_campaign_location.imps + VALUES(imps)
	, tvl_report_campaign_location.clks = tvl_report_campaign_location.clks + VALUES(clks)
	, tvl_report_campaign_location.vimps = tvl_report_campaign_location.vimps + VALUES(vimps)
	, tvl_report_campaign_location.vclks = tvl_report_campaign_location.vclks + VALUES(vclks)
	, tvl_report_campaign_location.cost = tvl_report_campaign_location.cost + VALUES(cost)
	, `update_timestamp` = VALUES(`create_timestamp`);
	
	#media
	INSERT INTO `tvl_report_media` (`timestamp`, `media_id`, `adspace_id`, `reqs`, `errs`
		, `bids`, `wins`, `imps`, `clks`, `vimps`, `vclks`, `income`, `create_timestamp`)
	SELECT `timestamp`, `media_id`, `adspace_id`, `reqs`, `errs`
		, `bids`, `wins`, `imps`, `clks`, `vimps`, `vclks`, `income`, unix_timestamp() AS `create_timestamp`
	FROM tvl_report_media_mem
	ON DUPLICATE KEY UPDATE tvl_report_media.reqs = tvl_report_media.reqs + VALUES(reqs)
	, tvl_report_media.errs = tvl_report_media.errs + VALUES(errs)
	, tvl_report_media.bids = tvl_report_media.bids + VALUES(bids)
  , tvl_report_media.wins = tvl_report_media.wins + VALUES(wins)
  , tvl_report_media.imps = tvl_report_media.imps + VALUES(imps)
	, tvl_report_media.clks = tvl_report_media.clks + VALUES(clks)
  , tvl_report_media.vimps = tvl_report_media.vimps + VALUES(vimps)
  , tvl_report_media.vclks = tvl_report_media.vclks + VALUES(vclks)
  , tvl_report_media.income = tvl_report_media.income + VALUES(income)
  , `update_timestamp` = VALUES(`create_timestamp`);

	#media with location
	INSERT INTO `tvl_report_media_location` (`timestamp`, `media_id`, `adspace_id`, `location`, `reqs`, `errs`
		, `bids`, `wins`, `imps`, `clks`, `vimps`, `vclks`, `income`, `create_timestamp`)
	SELECT `timestamp`, `media_id`, `adspace_id`, `location`, `reqs`, `errs`
		, `bids`, `wins`, `imps`, `clks`, `vimps`, `vclks`, `income`, unix_timestamp() AS `create_timestamp`
	FROM tvl_report_media_location_mem
	ON DUPLICATE KEY UPDATE tvl_report_media_location.reqs = tvl_report_media_location.reqs + VALUES(reqs)
	, tvl_report_media_location.errs = tvl_report_media_location.errs + VALUES(errs)
	, tvl_report_media_location.bids = tvl_report_media_location.bids + VALUES(bids)
  , tvl_report_media_location.wins = tvl_report_media_location.wins + VALUES(wins)
  , tvl_report_media_location.imps = tvl_report_media_location.imps + VALUES(imps)
	, tvl_report_media_location.clks = tvl_report_media_location.clks + VALUES(clks)
  , tvl_report_media_location.vimps = tvl_report_media_location.vimps + VALUES(vimps)
  , tvl_report_media_location.vclks = tvl_report_media_location.vclks + VALUES(vclks)
  , tvl_report_media_location.income = tvl_report_media_location.income + VALUES(income)
  , `update_timestamp` = VALUES(`create_timestamp`);
	
	#delete precessed records
	TRUNCATE tvl_report_campaign_mem;
	TRUNCATE tvl_report_campaign_location_mem;
	TRUNCATE tvl_report_media_mem;
	TRUNCATE tvl_report_media_location_mem;	

	IF t_error = 1 THEN
		ROLLBACK;
	ELSE 
		COMMIT;
	END IF;
	SELECT t_error,NOW();

END