SELECT *
FROM (SELECT *
      FROM test.unpivoted_weeks
      WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
        AND week = :week_num
        AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
        AND starttime > to_char(now() AT TIME ZONE 'Asia/Novosibirsk', 'HH24:MI')
        AND pk = (SELECT pk
                  FROM test.unpivoted_weeks
                  WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                    AND week = :week_num
                    AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                  ORDER BY starttime
                  LIMIT 1)
      UNION ALL
      SELECT *
      FROM test.unpivoted_weeks
      WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
        AND week = :week_num
        AND day > extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
        AND NOT EXISTS(SELECT 1
                       FROM test.unpivoted_weeks
                       WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                         AND week = :week_num
                         AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                         AND starttime > to_char(now() AT TIME ZONE 'Asia/Novosibirsk', 'HH24:MI')
                         AND pk = (SELECT pk
                                   FROM test.unpivoted_weeks
                                   WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                                     AND week = :week_num
                                     AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                                   ORDER BY starttime
                                   LIMIT 1)
          )
      ORDER BY week, day, starttime
     ) as tr
UNION ALL
SELECT *
FROM test.unpivoted_weeks
WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
  AND week > :week_num
  AND NOT EXISTS(SELECT 1
                 FROM (SELECT *
                       FROM test.unpivoted_weeks
                       WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                         AND week = :week_num
                         AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                         AND starttime > to_char(now() AT TIME ZONE 'Asia/Novosibirsk', 'HH24:MI')
                         AND pk = (SELECT pk
                                   FROM test.unpivoted_weeks
                                   WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                                     AND week = :week_num
                                     AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                                   ORDER BY starttime
                                   LIMIT 1)
                       UNION ALL
                       SELECT *
                       FROM test.unpivoted_weeks
                       WHERE group_name = (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                         AND week = :week_num
                         AND day > extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                         AND NOT EXISTS(SELECT 1
                                        FROM test.unpivoted_weeks
                                        WHERE group_name =
                                              (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                                          AND week = :week_num
                                          AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                                          AND starttime > to_char(now() AT TIME ZONE 'Asia/Novosibirsk', 'HH24:MI')
                                          AND pk = (SELECT pk
                                                    FROM test.unpivoted_weeks
                                                    WHERE group_name =
                                                          (SELECT group_name FROM users.usergroup WHERE user_id = :uid)
                                                      AND week = :week_num
                                                      AND day = extract(isodow from now() AT TIME ZONE 'Asia/Novosibirsk')
                                                    ORDER BY starttime
                                                    LIMIT 1)
                           )
                       ORDER BY week, day, starttime
                      ) as tr)
ORDER BY week, day, starttime
LIMIT 1