Download Link: https://assignmentchef.com/product/solved-bigdata-assignment1-spark-core-spark-sql
<br>
From the window you can see the sun shinning in a lovely autumn morning. Its Monday, 10am, and you are in the open plan office of a new start-up, OptimiseYourJourney, which will enter the market next year with a clear goal in mind: “<em>leverage Big Data technologies for improving the user experience in transportation”</em>.

The start-up is at a very early stage, and has no clear product in mind yet. However, they have offered a short-term internship in their Big Data Engineering Department to help them exploring the datasets, technologies and techniques that can be applied in their future products. They do not pay very well (0€ per hour), but you see this as a good opportunity to complement your knowledge in the module Big Data Processing you are studying at the moment, so you have decided to give it a go.

In the department meeting that has just finished your boss was particularly excited. During the weekend he was exploring the public datasets provided by the Irish government at <a href="https://data.gov.ie/">https://data.gov.ie/</a> and he found a transportation dataset named <strong>Dublin Bus GPS sample data from Dublin City Council (Insight Project)</strong>: <a href="https://data.gov.ie/dataset/dublin-bus-gps-sample-data-from-dublin-city-council-insight-project">https://data.gov.ie/dataset/dublin-bus</a><a href="https://data.gov.ie/dataset/dublin-bus-gps-sample-data-from-dublin-city-council-insight-project">gps-sample-data-from-dublin-city-council-insight-project</a> The original dataset contains 40+ millions of GPS data measurements collected from Dublin buses operating in January of 2013. Once extracted, it occupies ~5GB and is available at: <a href="http://opendata.dublincity.ie/TrafficOpenData/sir010113-310113.zip">http://opendata.dublincity.ie/TrafficOpenData/sir010113-310113.zip</a>

Your boss thinks this dataset provides a great opportunity to explore the potential of Spark Core and Spark SQL in analysing large datasets. He has already cleaned the dataset for you to perform some data analysis on it.

<strong><u>DATASET.</u></strong>

The new dataset (obtained after cleaning the original one) is provided to you in the folder <strong>Canvas =&gt; 5_Assignments =&gt; A01_dataset.zip</strong>.

It occupies ~3GB and contains 744 files, one per hour interval and day of the month:

<ul>

 <li>201301<strong>01</strong><strong>00</strong>.csv =&gt; Provides the data measurements of <strong>01</strong><strong><sup>st</sup></strong> of January 2013 in the hour interval <strong>[12am, 1am)</strong></li>

 <li>201301<strong>01</strong><strong>01</strong>.csv =&gt; Provides the data measurements of <strong>01</strong><strong><sup>st</sup></strong> of January 2013 in the hour interval <strong>[1am, 2am)</strong></li>

 <li>…</li>

 <li>201301<strong>01</strong><strong>08</strong>.csv =&gt; Provides the data measurements of <strong>01</strong><strong><sup>st</sup></strong> of January 2013 in the hour interval <strong>[8am, 9am)</strong></li>

 <li>201301<strong>01</strong><strong>09</strong>.csv =&gt; Provides the data measurements of <strong>01</strong><strong><sup>st</sup></strong> of January 2013 in the hour interval <strong>[9am, 10am)</strong></li>

 <li>…</li>

 <li>201301<strong>01</strong><strong>23</strong>.csv =&gt; Provides the data measurements of <strong>01</strong><strong><sup>st</sup></strong> of January 2013 in the hour interval <strong>[11pm, 12am)</strong></li>

 <li>201301<strong>02</strong><strong>00</strong>.csv =&gt; Provides the data measurements of <strong>02</strong><strong><sup>nd</sup></strong> of January 2013 in the hour interval <strong>[12am, 1am)</strong></li>

 <li>…</li>

 <li>201301<strong>31</strong><strong>23</strong>.csv =&gt; Provides the data measurements of <strong>31</strong><strong><sup>st</sup></strong> of January 2013 in the hour interval <strong>[11pm, 12am)</strong></li>

</ul>

Each row of a file contains the following fields:

<strong><em>Date , Bus_Line , Bus_Line_Pattern , Congestion , Longitude , Latitude , Delay , Vehicle , Closer_Stop , At_Stop</em></strong>

<ul>

 <li><strong>(00) Date</strong></li>

</ul>

◦ A String represents the date of the measurement with format

&lt;%Y-%m-%d %H:%M:%S&gt;

◦ Example: “2013-01-01 13:00:02” •  <strong>(01) Bus_Line</strong>

◦ An Integer representing the bus line.

◦ Example: 120

<ul>

 <li><strong>(02) Bus_Line_Pattern</strong></li>

</ul>

◦ A String identifier for the sequence of stops scheduled in the bus line (different buses of the same bus line can follow different sequence of stops in different iterations).

◦ Example: “027B1001” (it can also be empty “”). •   <strong>(03) Congestion</strong>

◦ An Integer representing whether the bus is at a traffic jam (No =&gt; 0 / Yes =&gt; 1) .

◦ Example: 0

<ul>

 <li><strong>(04) Longitude</strong></li>

</ul>

◦ A Float representing the longitude position of the bus.

◦ Example: -6.269634 • <strong>(05) Latitude</strong>

◦ A Float representing the latitude position of the bus. ◦ Example:  53.360504

<ul>

 <li><strong>(06) Delay</strong></li>

</ul>

◦ An integer representing the delay of the bus with respect to its schedule (measured in seconds). It is a negative value if the bus is ahead of schedule.

◦ Example:  90. • <strong>(07) Vehicle</strong>

◦ An integer identifier for the bus vehicle. ◦ Example:  33304. •    <strong>(08) Closer_Stop</strong>

◦ An integer identifier for the closest bus stop. ◦ Example:  7486.

<ul>

 <li><strong>(09) At_Stop_Stop</strong></li>

</ul>

◦ An integer representing whether the bus vehicle is at the bus stop right now (i.e., stopping at it for passengers to hop on / hop off). (No -&gt; 0 and Yes -&gt; 1)

◦ Example:  0.

<strong><u>TASKS / EXERCISES.</u></strong>

The tasks / exercises to be completed as part of the assignment are described in the next pages of the assignments.

<ul>

 <li>The following exercises are placed in the folder <strong>my_code:</strong>

  <ol>

   <li>py</li>

   <li>py</li>

   <li>py</li>

   <li>py</li>

   <li>py</li>

   <li>py</li>

   <li>py</li>

   <li>py</li>

  </ol></li>

</ul>

◦ <strong>Each exercise is worth 10 marks.</strong>

◦ On each exercise, your task is to complete the function <strong>my_main</strong> of the Python program. This function is in charge of specifying the Spark Job performing the desired data analysis.

◦ When programming my_main, you can create and call as many auxiliary functions as you need.

◦ Do not alter the Python main entry point (Python Execution Section) of the file.

◦ Do not alter the parameters passed to the function my_main.

◦ The function my_main must start with a creation operation textFile or read loading the dataset to Spark Core and Spark SQL, respectively.

◦ The function my_main must finish with an action operation collect gathering and printing by the screen the result of Spark Core / Spark SQL job.

<ul>

 <li>The following exercise is placed in the folder <strong>my_report:</strong></li>

</ul>

<ol start="9">

 <li>A01_report.odt</li>

</ol>

◦ <strong>The report is worth 20 marks.</strong>

◦ The report must contain a maximum of 1,000 words.




<strong><u>SUBMISSION DETAILS / SUBMISSION CODE OF CONDUCT.</u></strong>

Submit to Canvas by the 22<sup>nd</sup> of November, 11:59pm.

<ul>

 <li>Submissions up to 1 week late will have 10 marks deducted.</li>

 <li>Submissions up to 2 weeks late will have 20 marks deducted.</li>

</ul>

On submitting the assignment you adhere to the following declaration of authorship. If you have any doubt regarding the plagiarism policy discussed at the beginning of the semester do not hesitate in contacting me.

<strong>Declaration of Authorship</strong>

I, ___YOUR NAME___, declare that the work presented in this assignment titled ‘Assignment 1: Spark Core and Spark SQL’ is my own. I confirm that:

<ul>

 <li>This work was done wholly by me as part of my Msc. In Artificial Intelligence at Cork Institute of Technology.</li>

 <li>Where I have consulted the published work and source code of others, this is always clearly attributed.</li>

 <li>Where I have quoted from the work of others, the source is always given. With the exception of such quotations, this assignment source code and report is entirely my own work.</li>

</ul>

<strong><u>EXERCISE 1. </u></strong>

Let’s assume you live in Dublin, and you can see from your window the bus stop where the bus bringing you to work stops at every morning. The bus passes quite regularly, and your company is flexible with your starting time; thus, you can take the bus anytime from 7am to 10am.

Traffic jams in big cities are terrible, specially by the time you go to work, on weekdays at rush hour. This affects the bus schedule, making it difficult to predict its arrival time at the bus stop. Will the bus be on schedule, or delayed/ahead of time?

Moreover, is there a concrete hour in which the bus is, on average, more reliable? For example, of the possible hours where you can take the bus &lt; [7am – 8am), [8am – 9am), [9am – 10am) &gt;, what is the hour with smaller average deviation w.r.t. its scheduled time?

The dataset you got in your hands certainly allows you to infer this information; so let’s analyse it to find out.

<h1>EXERCISE: FORMAL DEFINITION</h1>

<strong>Given a program passing by parameters: </strong>

<ul>

 <li>The bus stop “bus_stop” (e.g., 279)</li>

 <li>The bus line “bus_line” (e.g., 40)</li>

 <li>A list of hours “hours_list” (e.g., [“07”, “08”, “09”] for the intervals [7am – 8am), [8am – 9am) and[9am – 10), resp.) <strong>Your task is to: </strong></li>

 <li>Compute the average delay of “bus_line” vehicles when stopping at “bus_stop” for each hour of”hours_list” during weekdays (you must discard any measurement taking place on a Saturday or Sunday).</li>

</ul>

The format of the solution computed must be:

<ul>

 <li>&lt; (H1, AD1), (H2, AD2), …, (Hn, ADn) &gt; where:</li>

 <li>Hi is one of the items of “hours_list”. E.g., [8am – 9am).</li>

 <li>ADi is the average delay for all “bus_line” vehicles stopping at “bus_stop” within that houron a weekday.</li>

 <li>&lt; (H1, AD1), (H2, AD2), …, (Hn, ADn) &gt; are also sorted by increasing order of ADi.</li>

</ul>

<h1>EXAMPLE – SMALL DATASET</h1>

Let’s assume we are interested in “bus_line” = 40, “bus_stop” = 279 and “hours_list” = [“08”, “09”].

Let’s consider the following measurements:

<em>Date,Bus_Line,Bus_Line_Pattern,Congestion,Longitude,Latitude,Delay,Vehicle,Closer_Stop,At_Stop</em>

<ul>

 <li>2013-01-19 08:00:36,40,015B1002,0,-6.258078,53.339279,544,33488,279,1 This measurement is not of interest to us: Weekend day.</li>

 <li>2013-01-09 08:00:36,41,015B1002,0,-6.258078,53.339279,544,33488,279,1 This measurement is not of interest to us: Wrong bus line.</li>

 <li>2013-01-09 08:00:36,40,015B1002,0,-6.258078,53.339279,544,33488,280,1 This measurement is not of interest to us: Wrong bus stop.</li>

 <li>2013-01-09 08:00:36,40,015B1002,0,-6.258078,53.339279,544,33488,279,0 This measurement is not of interest to us: The bus is not at stop.</li>

 <li>2013-01-09 18:00:36,40,015B1002,0,-6.258078,53.339279,300,33488,279,1 This measurement is not of interest to us: The hour is not in the list.</li>

 <li>2013-01-09 08:00:36,40,015B1002,0,-6.258078,53.339279,300,33488,279,1 This measurement is of interest to us. The bus reported 300 seconds delay.</li>

 <li>2013-01-09 08:25:36,40,015B1002,0,-6.258078,53.339279,-200,33488,279,1 This measurement is of interest to us. The bus reported -200 seconds ahead.</li>

 <li>2013-01-09 08:50:36,40,015B1002,0,-6.258078,53.339279,100,33488,279,1</li>

</ul>

This measurement is of interest to us. The bus reported 100 seconds delay. Now we have 3 measurements for buses passing in the interval [8am – 9am): +300 seconds, -200 seconds, +100 seconds, resp. The average delay time per bus is: + 66.67.

Let’s also assume our dataset is so small that it only contains the following measurements:

<em>Date,Bus_Line,Bus_Line_Pattern,Congestion,Longitude,Latitude,Delay,Vehicle,Closer_Stop,At_Stop</em>

<ul>

 <li>2013-01-09 08:00:36,40,015B1002,0,-6.258078,53.339279,300,33488,279,1</li>

 <li>2013-01-09 08:25:36,40,015B1002,0,-6.258078,53.339279,-200,33488,279,1</li>

 <li>2013-01-09 08:50:36,40,015B1002,0,-6.258078,53.339279,100,33488,279,1</li>

 <li>2013-01-19 08:00:36,40,015B1002,0,-6.258078,53.339279,300,33488,279,1</li>

 <li>2013-01-19 08:25:36,40,015B1002,0,-6.258078,53.339279,-200,33488,279,1</li>

 <li>2013-01-19 08:50:36,40,015B1002,0,-6.258078,53.339279,100,33488,279,1</li>

 <li>2013-01-29 08:00:36,40,015B1002,0,-6.258078,53.339279,300,33488,279,1</li>

 <li>2013-01-29 08:25:36,40,015B1002,0,-6.258078,53.339279,-200,33488,279,1</li>

 <li>2013-01-29 08:50:36,40,015B1002,0,-6.258078,53.339279,100,33488,279,1</li>

 <li>2013-01-09 09:00:36,40,015B1002,0,-6.258078,53.339279,100,33488,279,1</li>

 <li>2013-01-09 09:25:36,40,015B1002,0,-6.258078,53.339279,-100,33488,279,1</li>

 <li>2013-01-09 09:50:36,40,015B1002,0,-6.258078,53.339279,150,33488,279,1</li>

</ul>

<h1>SOLUTION EXAMPLE – SMALL DATASET</h1>

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (’09’, 50.0), (’08’, 66.67) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(’09’, 50.0)

(’08’, 66.67)

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(hour=’09’, averageDelay=50.0), Row(hour=’08’, averageDelay=66.67) &gt;

<ul>

 <li>solutionDF printed by the screen: Row(hour=’09’, averageDelay=50.0)</li>

</ul>

Row(hour=’08’, averageDelay=66.67)

<h1>SOLUTION EXAMPLE – ENTIRE DATASET</h1>

Given a program passing by parameters:

<ul>

 <li>The bus stop “bus_stop” = 279</li>

 <li>The bus line “bus_line” = 40</li>

 <li>A list of hours “hours_list” = [“07”, “08”, “09”]</li>

</ul>

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (’08’, 12.09), (’07’, 134.4), (’09’, 331.82) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(’08’, 12.09)

<ul>

 <li>(’07’, 134.4)</li>

 <li>(’09’, 331.82)</li>

</ul>

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(hour=’08’, averageDelay=12.09), Row(hour=’07’, averageDelay=134.4), Row(hour=’09’, averageDelay=331.82) &gt;

<ul>

 <li>solutionDF printed by the screen: Row(hour=’08’, averageDelay=12.09)</li>

</ul>

Row(hour=’07’, averageDelay=134.4)

Row(hour=’09’, averageDelay=331.82)

<strong><u>EXERCISE 2. </u></strong>

We use to think that each physical bus vehicle serves a single bus line. However, this does not need to be the case. It is perfectly possible (indeed, it happens quite often) that a bus a concrete physical bus vehicle (e.g., “33145”) serves one bus line (e.g., “25”) from 8am to 2pm, and another bus line (e.g., “66”) from 3pm to 9pm. A particular use-case of this could be increasing the amount of bus vehicles serving a concrete line at rush hours.

But, is this something that happens to all bus vehicles? Or, indeed something that happens every day?

Moreover, how many lines does a particular bus vehicle serve every day? Is there a particular busy day in which the bus vehicle serve a highest amount of different bus lines?

The dataset you got in your hands certainly allows you to infer this information; so let’s analyse it to find out.

<h1>EXERCISE: FORMAL DEFINITION</h1>

<strong>Given a program passing by parameters: </strong>

<ul>

 <li>The bus vehicle “vehicle_id” (e.g., 33145) <strong>Your task is to: </strong></li>

 <li>Compute the day(s) of the month in which this “vehicle_id” is serving the highest amount ofdifferent bus lines, and the IDs of such bus lines.</li>

</ul>

The format of the solution computed must be:

<ul>

 <li>&lt; (D1, LB1), (D2, LB2), …, (Dn, LBn) &gt; where:</li>

 <li>Di is the day of the month serving maximum amount of bus lines.</li>

 <li>LBi is the list of bus lines served in that concrete day Di.</li>

 <li>LBi is sorted by increasing order in the bus line Ids.</li>

 <li>Note that &lt; (D1, LB1), (D2, LB2), …, (Dn, LBn) &gt; will as many pairs (Di, LBi) as days serve the very same amount of max bus lines.</li>

 <li>If &lt; (D1, LB1), (D2, LB2), …, (Dn, LBn) &gt; contains more than one pair (Di, LBi), then the pairs are also sorted by increasing order of Di.</li>

</ul>

<h1>EXAMPLE – SMALL DATASET</h1>

Let’s assume we are interested in “vehicle_id” = 33145.

Let’s consider the following measurements:

<em>Date,Bus_Line,Bus_Line_Pattern,Congestion,Longitude,Latitude,Delay,Vehicle,Closer_Stop,At_Stop</em>

<ul>

 <li>2013-01-19 09:00:36,40,015B1002,0,-6.258078,53.339279,544,33245,279,1</li>

</ul>

This measurement is not of interest to us: Wrong vehicle ID.

<ul>

 <li>2013-01-09 09:00:36,40,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

</ul>

This measurement is of interest to us. The vehicle 33145 serves the line 40 on Wednesday 09th of January.

<ul>

 <li>2013-01-09 10:00:36,40,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

</ul>

This measurement is of interest to us, but it adds nothing new, as we already knew vehicle 33145 served the line 40 on Wednesday 09th of January.

Let’s also assume our dataset is so small that it only contains the following measurements:

<em>Date,Bus_Line,Bus_Line_Pattern,Congestion,Longitude,Latitude,Delay,Vehicle,Closer_Stop,At_Stop</em>

<ul>

 <li>2013-01-04 09:00:36,25,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-08 09:00:36,122,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-08 19:00:36,120,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-09 09:00:36,66,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-09 15:00:36,25,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-09 22:00:36,67,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-17 09:00:36,39,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-17 15:00:36,67,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-17 22:00:36,66,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-28 09:00:36,67,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-28 19:00:36,25,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

 <li>2013-01-30 09:00:36,122,015B1002,0,-6.258078,53.339279,544,33145,279,1</li>

</ul>

Thus, it is clear that the days our vehicle serves most lines are Wednesday 9th of January (where it serves 3 different lines, “25”, “66” and “67”), Thursday 17th of January (where it serves 3 different lines, “39”, “66” and “67”). The rest of days the vehicle serves less than 3 different bus lines.

<h1>SOLUTION EXAMPLE – SMALL DATASET</h1>

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (’09’, [25, 66, 67]), (’17’, [39, 66, 67]) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(’09’, [25, 66, 67])

(’17’, [39, 66, 67])

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(day=’09’, sortedBusLineIDs=[25, 66, 67]), Row(day=’17’, sortedBusLineIDs=[39, 66, 67]) &gt;

<ul>

 <li>solutionDF printed by the screen:</li>

</ul>

Row(day=’09’, sortedBusLineIDs=[25, 66, 67])

Row(day=’17’, sortedBusLineIDs=[39, 66, 67])

<h1>SOLUTION EXAMPLE – ENTIRE DATASET</h1>

Given a program passing by parameters:

– The bus vehicle “vehicle_id” = 33145

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (’02’, [38, 41, 171]), (’28’, [13, 140, 171]), (’29’, [40, 171, 238]) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(’02’, [38, 41, 171])

(’28’, [13, 140, 171])

(’29’, [40, 171, 238])

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(day=’02’, sortedBusLineIDs=[38, 41, 171]), Row(day=’28’, sortedBusLineIDs=[13,

140, 171]), Row(day=’29’, sortedBusLineIDs=[40, 171, 238]) &gt;

<ul>

 <li>solutionDF printed by the screen:</li>

</ul>

Row(day=’02’, sortedBusLineIDs=[38, 41, 171])

Row(day=’28’, sortedBusLineIDs=[13, 140, 171])

Row(day=’29’, sortedBusLineIDs=[40, 171, 238])

<strong><u>EXERCISE 3. </u></strong>

We said in exercise 1 that traffic jams are terrible and, by the time of writing exercise 3, we still have not changed our opinion. When it comes to transportation in big cities we all know that traffic congestion can be just past the corner.

But, is this something that happens every day and every hour? Most likely the answer is ‘yes’, every day, and every hour, there will be some buses reporting traffic congestion in some of their measurements.

Moreover, if we had a bar, a threshold, to consider what percentage of traffic congestion measurements is “too much”, what days and hours will go above that threshold?

The dataset you got in your hands certainly allows you to infer this information; so let’s analyse it to find out.

<h1>EXERCISE: FORMAL DEFINITION</h1>

<strong>Given a program passing by parameters: </strong>

<ul>

 <li>The aforementioned bar “threshold_percentage” (e.g., 10.0) <strong>Your task is to: </strong></li>

 <li>Compute the concrete days and hours having a percentage of measurements reportingcongestions above “threshold_percentage”.</li>

</ul>

The format of the solution computed must be:

<ul>

 <li>&lt; (D1, H1, P1), (D2, H2, P2), …, (Dn, Hn, Pn) &gt; where:</li>

 <li>Di is the day of the month (e.g., ’11’ for Friday 11th of January).</li>

 <li>Hi is the hour interval of the day (e.g, ’09’ for the interval [9am – 10am).</li>

 <li>Pi is the percentage of congestion measurements (e.g., 3.54).</li>

 <li>&lt; (D1, H1, P1), (D2, H2, P2), …, (Dn, Hn, Pn) &gt; are sorted by decreasing order of Pi.</li>

</ul>

<h1>EXAMPLE – SMALL DATASET</h1>

Let’s assume we are interested in “threshold_percentage” = 35.0.

Let’s also assume our dataset is so small that it only contains the following measurements:

<em>Date,Bus_Line,Bus_Line_Pattern,Congestion,Longitude,Latitude,Delay,Vehicle,Closer_Stop ,At_Stop</em>

<ul>

 <li>2013-01-01 09:15:36,41,015B1002,0,-6.258078,53.339279,200,33145,122,1</li>

 <li>2013-01-01 09:30:36,42,015B1002,0,-6.258078,53.339279,100,33245,120,1 • 2013-01-01 09:45:36,43,015B1002,1,-6.258078,53.339279,200,33345,66,1</li>

 <li>2013-01-02 09:15:36,41,015B1002,0,-6.258078,53.339279,200,33145,122,1</li>

 <li>2013-01-02 09:30:36,42,015B1002,1,-6.258078,53.339279,100,33245,120,1 • 2013-01-02 09:45:36,43,015B1002,1,-6.258078,53.339279,200,33345,66,1</li>

 <li>2013-01-03 09:15:36,41,015B1002,1,-6.258078,53.339279,200,33145,122,1</li>

 <li>2013-01-03 09:30:36,42,015B1002,1,-6.258078,53.339279,100,33245,120,1</li>

 <li>2013-01-03 09:45:36,43,015B1002,1,-6.258078,53.339279,200,33345,66,1</li>

</ul>

As we can see:

<ul>

 <li>On Tuesday 1st of January, [9am – 10am) there are 3 measurements and 1 report congestion =&gt; 33.33%.</li>

 <li>On Wednesday 2nd of January, [9am – 10am) there are 3 measurements and 2 report congestion =&gt; 66.67%.</li>

 <li>On Thursday 3rd of January, [9am – 10am) there are 3 measurements and 3 report congestion =&gt; 100.00%.</li>

</ul>

Thus, both Wednesday 2nd [9am – 10am) and Thursday 3rd [9am – 10am) are above “threshold_percentage”.

<h1>SOLUTION EXAMPLE – SMALL DATASET</h1>

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (’03’, ’09’, 100.0), (’02’, ’09’, 66.67) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(’03’, ’09’, 100.0)

(’02’, ’09’, 66.67)

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(day=’03’, hour=’09’, percentage=100.0), Row(day=’02’, hour=’09’, percentage=66.67) &gt;

<ul>

 <li>solutionDF printed by the screen:</li>

</ul>

Row(day=’03’, hour=’09’, percentage=100.0)

Row(day=’02’, hour=’09’, percentage=66.67)

<h1>SOLUTION EXAMPLE – ENTIRE DATASET</h1>

Given a program passing by parameters:

– The bar “threshold_percentage” = 10.0.

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (’28’, ’01’, 45.0), (’20’, ’02’, 23.06), (’20’, ’01’, 21.34) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(’28’, ’01’, 45.0)

(’20’, ’02’, 23.06)

(’20’, ’01’, 21.34)

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(day=’28’, hour=’01’, percentage=45.0), Row(day=’20’, hour=’02’, percentage=23.06), Row(day=’20’, hour=’01’, percentage=21.34) &gt;

<ul>

 <li>solutionDF printed by the screen: Row(day=’28’, hour=’01’, percentage=45.0)</li>

</ul>

Row(day=’20’, hour=’02’, percentage=23.06)

Row(day=’20’, hour=’01’, percentage=21.34)

<strong><u>EXERCISE 4. </u></strong>

When exploring a city as a tourist, there is nothing like a nice walk to observe things at a calm pace. However, in order to get a quick taste of a city, hop on and off to a bus can be the best option, especially when the distance among points of interest is very big.

Let’s suppose we are in the street and we divise a bus stop. Tired as we are after a few days of tourism walking long hours, we claim to ourselves: “Ok, decision made: for the first 30 minutes today we are not going to walk anymore. What time is it now? 9am. Perfect, so until 9.30am, no walking at all! Instead, look at this bus stop accross the street, let’s jump in to the first bus stopping at it, and see where does this bus bring us before 9.30am”.

The dataset you got in your hands certainly allows you to infer this information; so let’s analyse it to find out.

<h2>EXERCISE: FORMAL DEFINITION</h2>

<strong>Given a program passing by parameters: </strong>

<ul>

 <li>The current time “current_time” (e.g., “2013-01-10 08:59:59”)</li>

 <li>A bus stop “current_stop” (e.g., 1935)</li>

 <li>The time you allocate to yourself before you start walking again “seconds_horizon” (e.g.,1800 seconds, which is half an hour).</li>

</ul>

<strong>Your task is to: </strong>

<ul>

 <li>Compute the first bus stopping at “current_stop” and the list of bus stops it bring us withinthat time “seconds_horizon”.</li>

</ul>

The format of the solution computed must be:

<ul>

 <li>&lt; (V, [(T1, S1), (T2, S2), …, (Tn, Sn)]) &gt; where:</li>

 <li>V is the first bus vehicle stopping at “current_stop” after “current_time”.</li>

 <li>[(T1, S1), (T2, S2), …, (Tn, Sn)] is the list of other bus stops this bus vehicle stops at before the end of “current_time” + “seconds_horizon”, with Si being a bus stop and Ti the time stopping at it.</li>

 <li>The list includes “current_stop” as T1, so as to know the time we hopped on the bus.</li>

</ul>

<h1>EXAMPLE – SMALL DATASET</h1>

Let’s assume we are interested in “current_time” = “2013-01-10 08:59:59”, “current_stop” = 1935 and “seconds_horizon” = 1800.

Let’s consider the following measurements:

<em>Date,Bus_Line,Bus_Line_Pattern,Congestion,Longitude,Latitude,Delay,Vehicle,Closer_Stop ,At_Stop</em>

<ul>

 <li>2013-01-10 08:00:59,40,015B1002,0,-6.258078,53.339279,544,33488,1935,1 This measurement is not of interest to us: Its before our current time.</li>

 <li>2013-01-10 10:00:59,40,015B1002,0,-6.258078,53.339279,544,33488,1935,1</li>

</ul>

This measurement is not of interest to us: Its after our current time + seconds_horizon.

<ul>

 <li>2013-01-10 09:05:59,40,015B1002,0,-6.258078,53.339279,544,33488,279,1 This measurement is not of interest to us: Wrong bus stop.</li>

 <li>2013-01-10 09:05:59,50,015B1002,0,-6.258078,53.339279,544,33488,1935,0 This measurement is not of interest to us: Bus does not stop at bus stop.</li>

 <li>2013-01-20 09:05:59,40,015B1002,0,-6.258078,53.339279,544,33500,1935,1</li>

</ul>

This measurement is not of interest to us: Wrong day, so its after our current time + seconds_horizon.

<ul>

 <li>2013-01-10 09:05:59,40,015B1002,0,-6.258078,53.339279,544,33500,1935,1</li>

</ul>

If this is the earliest bus arriving then it is interest to us: we hop on that bus.

<ul>

 <li>2013-01-10 09:10:59,60,015B1002,0,-6.258078,53.339279,544,33600,1935,1</li>

</ul>

If this is not the earliest bus arriving then it is not of interest to us: we could have hopped on that bus, but another one arrived earlier, and thus we already hopped on it.

<ul>

 <li>2013-01-10 09:15:59,40,015B1002,0,-6.458078,53.439279,300,33500,244,1</li>

</ul>

If we hopped in this vehicle, this is of interest to us: our bus is stopping at a new stop.

<ul>

 <li>2013-01-10 09:25:59,40,015B1002,0,-6.658078,53.639279,200,33500,264,1</li>

</ul>

If we hopped in this vehicle, this is of interest to us: our bus is stopping at a new stop.

<ul>

 <li>2013-01-10 09:35:59,40,015B1002,0,-6.858078,53.839279,100,33500,284,1</li>

</ul>

Even if we hopped in this vehicle, this is no longer of interest to us: the time is after our current time + seconds_horizon.

Let’s also assume our dataset is so small that it only contains the following measurements:

<em>Date,Bus_Line,Bus_Line_Pattern,Congestion,Longitude,Latitude,Delay,Vehicle,Closer_Stop ,At_Stop</em>

<ul>

 <li>2013-01-10 08:00:59,40,015B1002,0,-6.258078,53.339279,544,33488,1935,1</li>

 <li>2013-01-10 10:00:59,40,015B1002,0,-6.258078,53.339279,544,33488,1935,1</li>

 <li>2013-01-10 09:05:59,40,015B1002,0,-6.258078,53.339279,544,33488,279,1</li>

 <li>2013-01-10 09:05:59,50,015B1002,0,-6.258078,53.339279,544,33488,1935,0</li>

 <li>2013-01-10 09:05:59,40,015B1002,0,-6.258078,53.339279,544,33500,1935,1</li>

 <li>2013-01-10 09:10:59,60,015B1002,0,-6.258078,53.339279,544,33600,1935,1</li>

 <li>2013-01-10 09:15:59,40,015B1002,0,-6.458078,53.439279,300,33500,244,1</li>

 <li>2013-01-10 09:25:59,40,015B1002,0,-6.658078,53.639279,200,33500,264,1</li>

 <li>2013-01-10 09:35:59,40,015B1002,0,-6.858078,53.839279,100,33500,284,1</li>

</ul>

As we can see:

<ul>

 <li>We hopped on bus vehicle 33500, arriving at stop 1935 at 2013-01-10 09:05:59.</li>

 <li>Bus vehicle 33500 arrived at stop 244 at 2013-01-10 09:15:59.</li>

 <li>Bus vehicle 33500 arrived at stop 264 at 2013-01-10 09:25:59.</li>

 <li>Bus vehicle 33500 arrived at stop 284 at 2013-01-10 09:35:59, but that was already outside of our “seconds_horizon”, so we hopped off the bus at previous stop 264.</li>

</ul>

Thus, our bus vehicle was 33500 and the stations we are interested at were:

<ul>

 <li>2013-01-10 09:05:59, stop 1935.</li>

 <li>2013-01-10 09:15:59, stop 244.</li>

 <li>2013-01-10 09:25:59, stop 264.</li>

</ul>

<h1>SOLUTION EXAMPLE – SMALL DATASET</h1>

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (33500, [(‘2013-01-10 09:05:59’, 1935), (‘2013-01-10 09:15:59’, 244), (‘2013-01-10 09:25:59’, 264)]) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(33500, [(‘2013-01-10 09:05:59’, 1935),

(‘2013-01-10 09:15:59’, 244),

(‘2013-01-10 09:25:59’, 264)

]

)

<em>Please note the RDD contains just one item, thus it is printed in just one line, with all the items of the list in a consecutive manner, not one item of the list per line.</em>

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(vehicleID=33500, stations=[Row(time=’2013-01-10 09:05:59′, stop=1935), Row(time=’2013-01-10 09:15:59′, stop=244), Row(time=’2013-01-10 09:25:59′, stop=264)]) &gt;

<ul>

 <li>solutionDF printed by the screen:</li>

</ul>

Row(vehicleID=33500, stations=[Row(time=’2013-01-10 09:05:59′, stop=1935),

Row(time=’2013-01-10 09:15:59′, stop=244),

Row(time=’2013-01-10 09:25:59′, stop=264)

]

)

<em>Please note the DF contains just one item, thus it is printed in just one line, with all the items of the list in a consecutive manner, not one item of the list per line.</em>

<h1>SOLUTION EXAMPLE – ENTIRE DATASET</h1>

Given a program passing by parameters:

<ul>

 <li>The current time “current_time” = “2013-01-10 08:59:59”</li>

 <li>A bus stop “current_stop” = 1935</li>

 <li>The time you allocate to yourself before you start walking again “seconds_horizon” = 1800</li>

</ul>

— SPARK CORE —

<ul>

 <li>solutionRDD:</li>

</ul>

&lt; (38022, [(‘2013-01-10 09:00:49’, 1935), (‘2013-01-10 09:04:07’, 1938), (‘2013-01-

10 09:16:28’, 1457), (‘2013-01-10 09:20:47’, 6094)]) &gt;

<ul>

 <li>solutionRDD printed by the screen:</li>

</ul>

(38022, [(‘2013-01-10 09:00:49’, 1935),

(‘2013-01-10 09:04:07’, 1938),

(‘2013-01-10 09:16:28’, 1457),

(‘2013-01-10 09:20:47’, 6094)

]

)

<em>Please note the RDD contains just one item, thus it is printed in just one line, with all the items of the list in a consecutive manner, not one item of the list per line.</em>

— SPARK SQL —

<ul>

 <li>solutionDF:</li>

</ul>

&lt; Row(vehicleID=38022, stations=[Row(time=’2013-01-10 09:00:49′, stop=1935), Row(time=’2013-01-10 09:04:07′, stop=1938), Row(time=’2013-01-10 09:16:28′, stop=1457), Row(time=’2013-01-10 09:20:47′, stop=6094)]) &gt;

<ul>

 <li>solutionDF printed by the screen (please note just one line is printed):</li>

 <li>Row(vehicleID=38022, stations=[Row(time=’2013-01-10 09:00:49′, stop=1935),</li>

</ul>

Row(time=’2013-01-10 09:04:07′, stop=1938),

Row(time=’2013-01-10 09:16:28′, stop=1457),

Row(time=’2013-01-10 09:20:47′, stop=6094)

]

)

<em>Please note the DF contains just one item, thus it is printed in just one line, with all the items of the list in a consecutive manner, not one item of the list per line.</em>

<strong><u>EXERCISE 5. </u></strong>

Write a report of up to 1,000 words where you present and discuss:

<ul>

 <li>A novel exercise to be included in the data analysis of the Dublin Bus dataset.</li>

</ul>

There is no need to implement the new exercise, you just need to discuss it in terms of:

<ul>

 <li>Its originality – It has to be different from the 4 exercises proposed.</li>

 <li>Its relevance – Include a potential use-case derived from the exercise you are proposing.</li>

 <li>Its viability</li>

</ul>

◦ Do not implement the exercise, but briefly discuss in natural language the main steps that would be needed so as to implement it.

◦ Include in the discussion whether you will choose to implement it in Spark Core or Spark SQL, justying your selection.

◦ Position the new exercise in terms of difficulty with respect to the four exercises proposed.