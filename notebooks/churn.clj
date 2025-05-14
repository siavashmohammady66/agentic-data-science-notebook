(ns notebooks.churn)
; # 1. Story behinde data
; ## 1.1. Problem
; ### 1.1.1. Problem Statement
; Understanding what factors affects customer churns & what are customer churn behaviors 
; ### 1.1.2. Limmitations
; There is no data on campaigns and marketing activities & if a customer leave course in the middle of it there is no data of it.
; ## 1.2. Data Sources
; 1. **Course**
;    * **Category**
;    * **Level**
; 2. **Course Calendar**
;    * **Type**
;    * **Capacity**
;    * **Gender**
;    * **Location**
;    * **CourseWeekDays**
; 3. **Customers**
;    * **Location**
;    * **Device**
; 4. **Course Registration**
;    * **Status**
;    * **Date**
; 5. **Teachers (Gender)**
;    * **Gender**
; 6. **Course Calendar Teachers**
; ## 1.3. Data Schema
; # 2. Hypothesizes
; 1. **Course Format**
; 2. **Geographic Behavior**
; 3. **Course registration patterns Impacts**
;    1. **Period between registrations**
;    2. **Course diversity**
;    3. **Course levels**
; 4. **Teacher Impact**
; 5. **Device-Specific Behavior Impacts**
; # 3. Data eexploration
