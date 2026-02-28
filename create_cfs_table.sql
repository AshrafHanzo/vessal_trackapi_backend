-- ============================================
-- Create CFS codes table in vessel_tracking DB
-- Run this in pgAdmin on vessal_track_production
-- Database: vessel_tracking > Schemas > public
-- ============================================

-- Create table
CREATE TABLE IF NOT EXISTS public.cfs_codes (
    id SERIAL PRIMARY KEY,
    s_no INTEGER NOT NULL,
    cfs_name VARCHAR(100) NOT NULL,
    cfs_code VARCHAR(20) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert CFS data (upsert: update if code already exists)
INSERT INTO public.cfs_codes (s_no, cfs_name, cfs_code) VALUES
    (1, 'ALL CARGO LOGISTICS', 'INMAA1AGL1'),
    (2, 'A.S SHIPPING', 'INMAA1ASS1'),
    (3, 'BALMER LAWRIE', 'INMAA1BLC1'),
    (4, 'BINNY', 'INMAA1BNL1'),
    (5, 'CONTINENTAL 1', 'INMAA1COW1'),
    (6, 'CHANDHRA', 'INMAA1CTO1'),
    (7, 'CWC MADHAVARAM', 'INMAA1CWC1'),
    (8, 'CWC ROYAPURAM', 'INMAA1CWC2'),
    (9, 'CWC VIRUGAMBAKKAM', 'INMAA1CWC3'),
    (10, 'CWC CROMPET', 'INMAA1CWC4'),
    (11, 'DRL', 'INMAA1DRL1'),
    (12, 'ECCT', 'INMAA1ECC1'),
    (13, 'GATE WAY', 'INMAA1GDL1'),
    (14, 'GERMAN', 'INMAA1GES1'),
    (15, 'GLOVIAS', 'INMAA1GLO1'),
    (16, 'ICBC', 'INMAA1ICB1'),
    (17, 'KAILASH', 'INMAA1KSS1'),
    (18, 'MAERSK', 'INMAA1MRK1'),
    (19, '0 YARD', 'INMAA1OYC1'),
    (20, 'SICAL', 'INMAA1SDL1'),
    (21, 'SUN GLOBAL', 'INMAA1SGL1'),
    (22, 'SATVA HITECH', 'INMAA1SHC1'),
    (23, 'SATVA 2', 'INMAA1SLP1'),
    (24, 'SANCO', 'INMAA1STL1'),
    (25, 'TRIWAY', 'INMAA1TCF1'),
    (26, 'THIRU RANI', 'INMAA1TRL1'),
    (27, 'VIKING CFS', 'INMAA1VIK1'),
    (28, 'VISHRUTHA', 'INMAA1VLT1'),
    (29, 'CONTINENTAL 2', 'INMAA1COW2'),
    (30, 'HIND TERMINAL', 'INMAA1SPL1'),
    (31, 'GALAXY', 'INMAA1CXC1'),
    (32, 'STP', 'INMAA1STP1'),
    (33, 'SUDHARSAN', 'INMAA1SUL1'),
    (34, 'NTR', 'INMAA1NTR1'),
    (35, 'CWC THIRUVATR', 'INMAA1CWC5'),
    (36, 'SCL', 'INMAA1SCL1')
ON CONFLICT (cfs_code) DO UPDATE SET
    cfs_name = EXCLUDED.cfs_name,
    s_no = EXCLUDED.s_no;

-- Verify
SELECT COUNT(*) AS total_records FROM public.cfs_codes;
SELECT * FROM public.cfs_codes ORDER BY s_no;
