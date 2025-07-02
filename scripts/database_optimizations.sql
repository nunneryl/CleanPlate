{\rtf1\ansi\ansicpg1252\cocoartf2821
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 -- Step 1: Add indexes for improved performance\
CREATE INDEX IF NOT EXISTS idx_restaurants_dba ON public.restaurants USING gin (dba gin_trgm_ops);\
CREATE INDEX IF NOT EXISTS idx_restaurants_zipcode ON public.restaurants (zipcode);\
CREATE INDEX IF NOT EXISTS idx_restaurants_grade ON public.restaurants (grade);\
\
-- Step 2: Update data type in violations table to match restaurants\
ALTER TABLE public.violations \
ALTER COLUMN inspection_date TYPE timestamp without time zone;\
\
-- Step 3: Add unique constraint to prevent duplicate violations\
ALTER TABLE public.violations \
ADD CONSTRAINT unique_violation UNIQUE (camis, inspection_date, violation_code);\
\
-- Step 4: Update foreign key constraint to add cascading delete\
ALTER TABLE public.violations \
DROP CONSTRAINT violations_camis_inspection_date_fkey;\
\
ALTER TABLE public.violations \
ADD CONSTRAINT violations_camis_inspection_date_fkey \
FOREIGN KEY (camis, inspection_date) \
REFERENCES public.restaurants(camis, inspection_date) \
ON DELETE CASCADE;}