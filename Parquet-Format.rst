Nested Encoding
================
The bitwidth for the RLE encoding of the definition and repition levels is calculated for each column individually. 
For column X: If definition levels have a max value of d, then RLE encoding of the definition levels use bitwidth(d) bits per value. Same for the repition levels.

In a DataPage the levels can be omitted under the following conditions:
* R-Levels: The column is not nested (top-level column)
* D-Levels: The column is required and is not nested (top-level column)
