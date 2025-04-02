-- Tabela de Operadoras Ativas com tamanhos adequados
CREATE TABLE IF NOT EXISTS operators (
                                         ans_registration VARCHAR(20) PRIMARY KEY,
    cnpj VARCHAR(20),
    legal_name VARCHAR(255),
    trade_name VARCHAR(255),
    modality VARCHAR(100),
    address VARCHAR(255),
    number VARCHAR(20),
    complement VARCHAR(100),
    neighborhood VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    area_code VARCHAR(5),
    phone VARCHAR(20),
    fax VARCHAR(20),
    email VARCHAR(100),
    representative VARCHAR(255),
    representative_role VARCHAR(100),
    created_at DATE
    );

-- Tabela de Demonstrações Financeiras
CREATE TABLE IF NOT EXISTS financial_reports (
                                                 id SERIAL PRIMARY KEY,
                                                 ans_registration VARCHAR(20),
    report_date DATE,
    report_type VARCHAR(100),
    total_revenue NUMERIC(15, 2),
    total_expenses NUMERIC(15, 2),
    balance NUMERIC(15, 2),
    FOREIGN KEY (ans_registration) REFERENCES operators(ans_registration)
    );