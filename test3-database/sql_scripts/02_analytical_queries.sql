-- Consulta 1: Top 10 operadoras com maiores despesas no último trimestre
SELECT
    o.legal_name,
    SUM(f.total_expenses) AS total_expenses
FROM
    financial_reports f
        JOIN
    operators o ON f.ans_registration = o.ans_registration
WHERE
    f.report_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY
    o.legal_name
ORDER BY
    total_expenses DESC
    LIMIT 10;

-- Consulta 2: Top 10 operadoras com maiores despesas no último ano
SELECT
    o.legal_name,
    SUM(f.total_expenses) AS total_expenses,
    (SUM(f.total_expenses) - LAG(SUM(f.total_expenses)) OVER (ORDER BY SUM(f.total_expenses) DESC)) AS difference
FROM
    financial_reports f
        JOIN
    operators o ON f.ans_registration = o.ans_registration
WHERE
    f.report_date >= CURRENT_DATE - INTERVAL '1 year'
GROUP BY
    o.legal_name
ORDER BY
    total_expenses DESC
    LIMIT 10;