USE openfda_serverless;
GO

SELECT
    classification,
    COUNT(*) AS recall_count
FROM dbo.vw_openfda_recall_curated
GROUP BY classification
ORDER BY recall_count DESC;
GO

SELECT TOP 10
    recalling_firm,
    COUNT(*) AS recall_count
FROM dbo.vw_openfda_recall_curated
WHERE recalling_firm IS NOT NULL
GROUP BY recalling_firm
ORDER BY recall_count DESC, recalling_firm;
GO

SELECT
    report_year,
    report_month,
    COUNT(*) AS recall_count
FROM dbo.vw_openfda_recall_curated
GROUP BY report_year, report_month
ORDER BY report_year DESC, report_month DESC;
GO

SELECT TOP 15
    state,
    COUNT(*) AS recall_count
FROM dbo.vw_openfda_recall_curated
WHERE state IS NOT NULL
GROUP BY state
ORDER BY recall_count DESC, state;
GO

SELECT TOP 20
    report_date,
    classification,
    recalling_firm,
    state,
    recall_number,
    LEFT(reason_for_recall, 200) AS reason_for_recall_short
FROM dbo.vw_openfda_recall_curated
ORDER BY report_date DESC, recall_number DESC;
GO

SELECT
    validation_error,
    COUNT(*) AS invalid_record_count
FROM dbo.vw_openfda_recall_invalid
GROUP BY validation_error
ORDER BY invalid_record_count DESC;
GO
