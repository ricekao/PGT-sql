-- 改 bright_checkin_top5000
-- 改 bright_top5000 
-- 改 bright_friends_top5000

-- rice_schema --> rice_brightkite
-- gowalla --> bright   (venue, checkin_count)
-- threshold: gowalla:4.28 ; brightkite: 2.95




-- STEP 1: calculate personal_background

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_personal(
	`user_id` int(11) NOT NULL, 
	`venue_id` varchar(50)  NOT NULL, 
	`frequency` int(11)  NOT NULL,
	`checkin_count` int(11)  NOT NULL,
	`p_prob` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1; 

insert into rice_brightkite.pgt_bright_top5000_personal(
	SELECT user.user_id, user.venue_id, user.frequency, bright.checkin_count, 
    (user.frequency / bright.checkin_count)
	FROM (
        	SELECT user_id, venue_id, COUNT( * ) AS frequency
			FROM  `bright_checkin_top5000` 
			GROUP BY user_id, venue_id
		)user
	JOIN rice_brightkite.bright_checkin_count bright 
    	ON user.user_id = bright.user_id
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_personal` 
ADD INDEX `user_id` (`user_id` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `frequency` (`frequency` ASC),
ADD INDEX `checkin_count` (`checkin_count` ASC),
ADD INDEX `p_prob` (`p_prob` ASC);






-- STEP 2: calculate global_background

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_global_temp1(
	`user_id` int(11) NOT NULL, 
	`venue_id` varchar(50)  NOT NULL, 
	`frequency` int(11)  NOT NULL,
	`venue_count` int(11)  NOT NULL,
	`l_prob` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_global_temp1(
	select personal.user_id, personal.venue_id, personal.frequency, venue.frequency,
			(personal.frequency/venue.frequency)
	from rice_brightkite.bright_venue as venue
		join rice_brightkite.pgt_bright_top5000_personal as personal
			on venue.id = personal.venue_id
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_global_temp1` 
ADD INDEX `user_id` (`user_id` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `frequency` (`frequency` ASC),
ADD INDEX `venue_count` (`venue_count` ASC),
ADD INDEX `l_prob` (`l_prob` ASC);


CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_global(
	`venue_id` varchar(50)  NOT NULL,
	`l_entropy` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_global(
	select venue_id, -(sum(l_prob * log(l_prob)))
	from rice_brightkite.pgt_bright_top5000_global_temp1
	group by venue_id
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_global` 
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `l_entropy` (`l_entropy` ASC);







-- STEP 3: calculate w_g

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_global_2(
	`venue_id` varchar(50)  NOT NULL,
	`l_entropy` double  NOT NULL,
	`w_g` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_global_2(
	select venue_id, l_entropy, exp(-(l_entropy))
	from rice_brightkite.pgt_bright_top5000_global
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_global_2` 
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `l_entropy` (`l_entropy` ASC),
ADD INDEX `w_g` (`w_g` ASC);





-- STEP 4: calculate co-occurrence	
-- We already have this table: bright_top5000 in Java at "runCoOccurrence" function
-- We add indexes here

ALTER TABLE `rice_brightkite`.`bright_top5000` 
ADD INDEX `user_1` (`user_1` ASC),
ADD INDEX `user_2` (`user_2` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `frequency` (`frequency` ASC),
ADD INDEX `avg_checkin_time` (`avg_checkin_time` ASC);





-- STEP 5: calculate max(w_p): the weight of personal background

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_wp_temp1(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
	`venue_id` varchar(50)  NOT NULL, 
	`p_prob1` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1; 

insert into rice_brightkite.pgt_bright_top5000_wp_temp1(
select co_occur.user_1, co_occur.user_2, co_occur.venue_id, personal.p_prob
from rice_brightkite.bright_top5000 as co_occur
		join rice_brightkite.pgt_bright_top5000_personal as personal
			on co_occur.user_1 = personal.user_id and co_occur.venue_id = personal.venue_id
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_wp_temp1` 
ADD INDEX `user_1` (`user_1` ASC),
ADD INDEX `user_2` (`user_2` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `p_prob_1` (`p_prob1` ASC);


CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_wp_temp2(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
	`venue_id` varchar(50)  NOT NULL, 
	`p_prob1` double  NOT NULL,
	`p_prob2` double  NOT NULL,
	`w_p` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1; 

insert into rice_brightkite.pgt_bright_top5000_wp_temp2(
	select co_occur.user_1, co_occur.user_2, co_occur.venue_id, co_occur.p_prob1, personal.p_prob, 
			-log(co_occur.p_prob1 * personal.p_prob)
	from rice_brightkite.pgt_bright_top5000_wp_temp1 as co_occur
			join rice_brightkite.pgt_bright_top5000_personal as personal
				on co_occur.user_2 = personal.user_id and co_occur.venue_id = personal.venue_id
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_wp_temp2` 
ADD INDEX `user_1` (`user_1` ASC),
ADD INDEX `user_2` (`user_2` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `p_prob1` (`p_prob1` ASC),
ADD INDEX `p_prob2` (`p_prob2` ASC),
ADD INDEX `w_p` (`w_p` ASC);


-- group by co_occur.user_1, co_occur.user_2, co_occur.venue_id

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_wp_max(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
	`max_w_p` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1; 

insert into rice_brightkite.pgt_bright_top5000_wp_max(
	select user_1, user_2, max(w_p)
	from rice_brightkite.pgt_bright_top5000_wp_temp2
	group by user_1, user_2
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_wp_max` 
ADD INDEX `user_1` (`user_1` ASC),
ADD INDEX `user_2` (`user_2` ASC),
ADD INDEX `max_w_p` (`max_w_p` ASC);




-- STEP 6: calculate temporal facotr: w_t
CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_temporal(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
	`venue_id` varchar(50)  NOT NULL,
	`frequency` int(11) NOT NULL, 
    `avg_checkin_time` double  NOT NULL,
	`w_t` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


-- Calculate time diff between the current event and previous event using "avg_checkin_time"

insert into rice_brightkite.pgt_bright_top5000_temporal (
	select * from
	(
		select  a.user_1, a.user_2, a.venue_id, a.frequency, a.avg_checkin_time,
			case when a.user_1 = b.user_1 
					and a.user_2 = b.user_2
					and a.num - b.num = 1 
				then 1 - exp( -0.2 * abs(a.avg_checkin_time - b.avg_checkin_time)) 
		        else 1 END as w_t
		from
			(SELECT user_1, user_2, venue_id, frequency, avg_checkin_time, @row_num := @row_num + 1 as 'Num' 
		    FROM rice_brightkite.bright_top5000
				JOIN (SELECT @row_num := 0 FROM DUAL) sub1
			-- where user_1 = 185
			order by user_1, user_2, avg_checkin_time) a
		join
			(SELECT user_1, user_2, venue_id, frequency, avg_checkin_time, @row_num2 := @row_num2 + 1 as 'Num' 
		    FROM rice_brightkite.bright_top5000
				JOIN (SELECT @row_num2 := 0 FROM DUAL) sub2
			-- where user_1 = 185
			order by user_1, user_2, avg_checkin_time) b
		    
		where (a.user_1 = b.user_1 and a.user_2 = b.user_2)
			 and ((a.Num = b.Num) or (a.Num - b.Num = 1))

	) temp
	group by temp.user_1, temp.user_2, temp.avg_checkin_time
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_temporal` 
ADD INDEX `user_1` (`user_1` ASC),
ADD INDEX `user_2` (`user_2` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `frequency` (`frequency` ASC),
ADD INDEX `avg_checkin_time` (`avg_checkin_time` ASC),
ADD INDEX `w_t` (`w_t` ASC);


-- aggregate co-occurrence_temporal with max_w_p and w_g

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_aggregated_temp1(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
	`venue_id` varchar(50)  NOT NULL,
	`frequency` int(11) NOT NULL, 
    `avg_checkin_time` double  NOT NULL,
	`w_t` double  NOT NULL,
	`max_w_p` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_aggregated_temp1 (
	select temporal.user_1, temporal.user_2, temporal.venue_id, temporal.frequency, 
			temporal.avg_checkin_time, temporal.w_t, personal.max_w_p
	from rice_brightkite.pgt_bright_top5000_temporal as temporal
		join rice_brightkite.pgt_bright_top5000_wp_max as personal
			on temporal.user_1 = personal.user_1 and temporal.user_2 = personal.user_2

);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_aggregated_temp1` 
ADD INDEX `user_1` (`user_1` ASC),
ADD INDEX `user_2` (`user_2` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `frequency` (`frequency` ASC),
ADD INDEX `avg_checkin_time` (`avg_checkin_time` ASC),
ADD INDEX `w_t` (`w_t` ASC),
ADD INDEX `max_w_p` (`max_w_p` ASC);

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_aggregated_temp2(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
	`venue_id` varchar(50)  NOT NULL,
	`frequency` int(11) NOT NULL, 
    `avg_checkin_time` double  NOT NULL,
	`w_t` double  NOT NULL,
	`w_g` double  NOT NULL,
	`max_w_p` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_aggregated_temp2 (
	select temp.user_1, temp.user_2, temp.venue_id, temp.frequency, 
			temp.avg_checkin_time, temp.w_t, global.w_g, temp.max_w_p
	from rice_brightkite.pgt_bright_top5000_aggregated_temp1 as temp
		join rice_brightkite.pgt_bright_top5000_global_2 as global
			on temp.venue_id = global.venue_id
);

ALTER TABLE `rice_brightkite`.`pgt_bright_top5000_aggregated_temp2` 
ADD INDEX `user_1` (`user_1` ASC),
ADD INDEX `user_2` (`user_2` ASC),
ADD INDEX `venue_id` (`venue_id` ASC),
ADD INDEX `frequency` (`frequency` ASC),
ADD INDEX `avg_checkin_time` (`avg_checkin_time` ASC),
ADD INDEX `w_t` (`w_t` ASC),
ADD INDEX `w_g` (`w_g` ASC),
ADD INDEX `max_w_p` (`max_w_p` ASC);

-- calculate social strength
CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_aggregated(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
    `social_strength` double  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_aggregated (
	select user_1, user_2, (max_w_p * sum(w_g * w_t))
	from rice_brightkite.pgt_bright_top5000_aggregated_temp2
	group by user_1, user_2
);





-- STEP 7: Evaluate with threshold and friendship table 
-- rice_brightkite.pgt threshold = 4.28 (bright), 2.95 (brightkite)

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_aggregated_with_friends_temp1(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
    `social_strength` double  NOT NULL,
    `predicted_tie` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_aggregated_with_friends_temp1 (
	SELECT * , 
		case when social_strength >= 2.95
			then 1
            else 0 END as predicted_tie
	FROM rice_brightkite.pgt_bright_top5000_aggregated
);


CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_aggregated_with_friends(
	`user_1` int(11) NOT NULL, 
	`user_2` int(11) NOT NULL, 
    `social_strength` double  NOT NULL,
    `predicted_tie` int(11) NOT NULL,
    `friendship` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_aggregated_with_friends (
	select * , 
  		(SELECT EXISTS( SELECT *  
  				    	FROM rice_brightkite.bright_friends_top5000  										
  				    	WHERE rice_brightkite.bright_friends_top5000.user_1 = rice_brightkite.pgt_bright_top5000_aggregated_with_friends_temp1.user_1   
  				    	and rice_brightkite.bright_friends_top5000.user_2 = rice_brightkite.pgt_bright_top5000_aggregated_with_friends_temp1.user_2
  				    	)
  		) as friendship
  	from rice_brightkite.pgt_bright_top5000_aggregated_with_friends_temp1
);





-- STEP 8: calculate precision, recall

CREATE TABLE IF NOT EXISTS rice_brightkite.pgt_bright_top5000_evaluation(
	`predicted_tie` int(11) NOT NULL, 
	`friendship` int(11) NOT NULL, 
    `correct` int(11)  NOT NULL,
    `num_of_user_pair` int(11) NOT NULL,
    `acc` int(11) NOT NULL,
    `precision` double NOT NULL,
    `recall` double NOT NULL,
    `accuracy` double NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into rice_brightkite.pgt_bright_top5000_evaluation (
	select *, correct/predicted_tie as precis, correct/friendship as recall, acc/num_of_user_pair as accuracy
	from
	(select
		(SELECT count(*) 
			FROM rice_brightkite.pgt_bright_top5000_aggregated_with_friends
			where predicted_tie = 1) as predicted_tie,
		(SELECT count(*) 
			FROM rice_brightkite.pgt_bright_top5000_aggregated_with_friends
			where friendship = 1) as friendship,
		(SELECT count(*) 
			FROM rice_brightkite.pgt_bright_top5000_aggregated_with_friends
			where predicted_tie = 1 and friendship = 1) as correct,
		(SELECT count(*) 
			FROM rice_brightkite.pgt_bright_top5000_aggregated_with_friends
		) as num_of_user_pair,
		(SELECT count(*) 
			FROM rice_brightkite.pgt_bright_top5000_aggregated_with_friends
			where (predicted_tie = 1 and friendship = 1)
				or (predicted_tie = 0 and friendship = 0)) as acc
	from rice_brightkite.pgt_bright_top5000_aggregated_with_friends) temp
);








