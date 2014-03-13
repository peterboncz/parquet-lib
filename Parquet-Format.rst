Nested Encoding
================
The bitwidth for the RLE encoding of the definition and repition levels is calculated for each column individually. 
For column X: If definition levels have a max value of d, then RLE encoding of the definition levels use bitwidth(d) bits per value. Same for the repition levels.
