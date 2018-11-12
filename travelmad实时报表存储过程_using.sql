BEGIN

	DECLARE request_time TIMESTAMP;

	DECLARE tracker_time TIMESTAMP;

	DECLARE t_error INTEGER DEFAULT 0;

	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;

	

	START TRANSACTION;

	SET @request_time = (

		SELECT MAX(create_timestamp)

		FROM tvl_report_request_mem

	);

	SET @tracker_time = (

		SELECT MAX(create_timestamp)

		FROM tvl_report_tracker_mem

	);

	SELECT sleep(1), @request_time, @tracker_time, NOW();



	IF @request_time IS NULL

	AND @tracker_time IS NULL THEN

		SELECT "目前没有已经处理完的数据" AS msg;

	ELSE 



		#campaign ---->request

		INSERT INTO `tvl_report_campaign_rt` (`timestamp`, `project_id`, `campaign_id`, `material_id`, `bids`

			, `wins`, `create_timestamp`)

		SELECT `timestamp`, `project_id`, `campaign_id`, `material_id`

			, SUM(bids) AS `bids`, SUM(wins) AS `wins`, unix_timestamp() AS create_timestamp

		FROM tvl_report_request_mem

		WHERE create_timestamp <= @request_time and project_id > 0

		GROUP BY `timestamp`, `project_id`, `campaign_id`, `material_id`

		ON DUPLICATE KEY UPDATE tvl_report_campaign_rt.bids = tvl_report_campaign_rt.bids + VALUES(bids)

		, tvl_report_campaign_rt.wins = tvl_report_campaign_rt.wins + VALUES(wins)

		, `update_timestamp` = VALUES(`create_timestamp`);

		

		#campaign ---->track

		INSERT INTO `tvl_report_campaign_rt` (`timestamp`, `project_id`, `campaign_id`, `material_id`, `imps`

			, `clks`, `vimps`, `vclks`, `cost`, `create_timestamp`)

		SELECT `timestamp`, `project_id`, `campaign_id`, `material_id`

			, SUM(imps) AS `imps`, SUM(clks) AS `clks`

			, SUM(vimps) AS `vimps`, SUM(vclks) AS `vclks`

			, SUM(cost) AS `cost`, unix_timestamp() AS create_timestamp

		FROM tvl_report_tracker_mem

		WHERE create_timestamp <= @tracker_time and project_id > 0

		GROUP BY `timestamp`, `project_id`, `campaign_id`, `material_id`

		ON DUPLICATE KEY UPDATE tvl_report_campaign_rt.imps = tvl_report_campaign_rt.imps + VALUES(imps)

		, tvl_report_campaign_rt.clks = tvl_report_campaign_rt.clks + VALUES(clks)

		, tvl_report_campaign_rt.vimps = tvl_report_campaign_rt.vimps + VALUES(vimps)

		, tvl_report_campaign_rt.vclks = tvl_report_campaign_rt.vclks + VALUES(vclks)

		, tvl_report_campaign_rt.cost = tvl_report_campaign_rt.cost + VALUES(cost)

		, `update_timestamp` = VALUES(`create_timestamp`);

		

		#media ---->request

		INSERT INTO `tvl_report_media_rt` (`timestamp`, `media_id`, `adspace_id`, `reqs`, `errs`

			, `bids`, `wins`, `create_timestamp`)

		SELECT `timestamp`, `media_id`, `adspace_id`, SUM(reqs) AS `reqs`

			, SUM(errs) AS `errs`, SUM(bids) AS `bids`

			, SUM(wins) AS `wins`, unix_timestamp() AS create_timestamp

		FROM tvl_report_request_mem

		WHERE create_timestamp <= @request_time and media_id > 0

		GROUP BY `timestamp`, `media_id`, `adspace_id`

		ON DUPLICATE KEY UPDATE tvl_report_media_rt.reqs = tvl_report_media_rt.reqs + VALUES(reqs)

		, tvl_report_media_rt.errs = tvl_report_media_rt.errs + VALUES(errs)

		, tvl_report_media_rt.bids = tvl_report_media_rt.bids + VALUES(bids)

		, tvl_report_media_rt.wins = tvl_report_media_rt.wins + VALUES(wins)

		, `update_timestamp` = VALUES(`create_timestamp`);

		

		#media ---->track

		INSERT INTO `tvl_report_media_rt` (`timestamp`, `media_id`, `adspace_id`, `imps`, `clks`

			, `vimps`, `vclks`, `income`, `create_timestamp`)

		SELECT `timestamp`, `media_id`, `adspace_id`, SUM(imps) AS `imps`

			, SUM(clks) AS `clks`, SUM(vimps) AS `vimps`

			, SUM(vclks) AS `vclks`, SUM(income) AS `income`, unix_timestamp() AS create_timestamp

		FROM tvl_report_tracker_mem

		WHERE create_timestamp <= @tracker_time and media_id > 0

		GROUP BY `timestamp`, `media_id`, `adspace_id`

		ON DUPLICATE KEY UPDATE tvl_report_media_rt.imps = tvl_report_media_rt.imps + VALUES(imps)

		, tvl_report_media_rt.clks = tvl_report_media_rt.clks + VALUES(clks)

		, tvl_report_media_rt.vimps = tvl_report_media_rt.vimps + VALUES(vimps)

		, tvl_report_media_rt.vclks = tvl_report_media_rt.vclks + VALUES(vclks)

		, tvl_report_media_rt.income = tvl_report_media_rt.income + VALUES(income)

		, `update_timestamp` = VALUES(`create_timestamp`);

		

		#delete the records have been processed

		DELETE FROM tvl_report_request_mem WHERE create_timestamp <= @request_time;

		DELETE FROM tvl_report_tracker_mem WHERE create_timestamp <= @tracker_time;

	END IF;

	IF t_error = 1 THEN

		ROLLBACK;

	ELSE 

		COMMIT;

	END IF;

	SELECT t_error, NOW();

END