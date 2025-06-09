SELECT
    CAST(T.CONFIRMED_DELETED_DATE AS INT) CONFIRMED_DELETED_DATE
   ,CAST(T.DELETED_DATE AS DATETIME)      DELETED_DATE
   ,T.CONFIRMED_DELETED_REMARK            CONFIRMED_DELETED_REMARK
FROM    (VALUES
             /*******************************************
    Please ADD your confirmed DELETE Date below
    separate using comma
    (1, Date, reason), (1, Date, reason), ...
    *******************************************/
             (1, '2024-04-18', 'Hung King Day')
            ,(1, '2024-04-30', 'Victory Day')
            ,(1, '2024-05-01', 'Labor Day')
            ,(1, '2024-09-02', 'Le 2-9')
            ,(1, '2024-09-03', 'Le 2-9')
            ,(1, '2025-01-01', 'Solar New Years Day')
            ,(1, '2025-01-27', 'Lunar New Year') -- dieu chinh thay the cho ngay 2025-02-01
            ,(1, '2025-01-28', 'Lunar New Year')
            ,(1, '2025-01-29', 'Lunar New Year')
            ,(1, '2025-01-30', 'Lunar New Year')
            ,(1, '2025-01-31', 'Lunar New Year')
            ,(1, '2025-02-01', 'Lunar New Year') -- Nam bổ sung thêm 1 ngày cần xóa nữa là Thứ 7, 2025-02-01 sau Tết
            ,(1, '2025-04-07', 'Hung King Day')
            ,(1, '2025-04-30', 'Victory Day')
            ,(1, '2025-05-01', 'Labor Day')
            ,(1, '2025-09-01', 'Independence Day')
            ,(1, '2025-09-02', 'Independence Day')) T (CONFIRMED_DELETED_DATE, DELETED_DATE, CONFIRMED_DELETED_REMARK)