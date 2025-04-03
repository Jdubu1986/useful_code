CASE 
    WHEN Table.Column IN ('6Z', '6R') 
        AND Table.Colum2 IN ('3', '3R', '4', '4R', '10', '11', '12', '13') 
    THEN 1  -- If Table.Column is '6Z' or '6R' AND Table.Colum2 is one of the specified values, then return 1
    ELSE 
        CASE 
            WHEN LEAD(Table.Column, 1) OVER (PARTITION BY Table.Column3 ORDER BY Table.Column4) = '01' 
                AND LEAD(Table.Column4, 1) OVER (PARTITION BY Table.Column3 ORDER BY Table.Column4) IS NOT NULL
            THEN 1  -- If the next Table.Column value (ordered by Table.Column4, partitioned by Table.Column3) is '01' AND the next Table.Column4 is not null, return 1
            ELSE 0  -- Otherwise, return 0
        END
END

CASE 
    WHEN LEAD(Table.Column5) OVER (PARTITION BY Table.Column3 ORDER BY Table.Column4) IS NULL 
        THEN 
            (CASE 
                WHEN LEAD(Table.Column6) OVER (PARTITION BY Table.Column3 ORDER BY Table.Column4) IS NOT NULL 
                THEN 1  -- If the next Table.Column6 value (ordered by Table.Column4, partitioned by Table.Column3) is not null, return 1
                ELSE 0  -- Otherwise, return 0
            END)
        ELSE 0  -- If the next Table.Column5 value (ordered by Table.Column4, partitioned by Table.Column3) is not null, return 0
END
