CREATE TABLE [Gold_Layer].[dim_calendar] (
    [date]       DATE           NULL,
    [day_num]    BIGINT         NULL,
    [day_name]   VARCHAR (8000) NULL,
    [week]       BIGINT         NULL,
    [iso_week]   BIGINT         NULL,
    [iso_year]   BIGINT         NULL,
    [month]      BIGINT         NULL,
    [month_name] VARCHAR (8000) NULL,
    [quarter]    VARCHAR (8000) NULL,
    [year]       BIGINT         NULL
);


GO