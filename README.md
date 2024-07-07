

### YouTube Subtitle Analysis with PySpark

This script utilizes PySpark and YouTubeTranscriptApi to retrieve and analyze subtitles from YouTube videos based on specific keywords. Here's a breakdown of its functionality:

#### Features:

1. **Subtitle Retrieval**: Fetches subtitles from a YouTube video using `YouTubeTranscriptApi`.
2. **Subtitle Filtering**: Filters subtitles based on predefined keywords.
3. **Word Count Analysis**: Counts occurrences of specific keywords within the filtered subtitles.
4. **PySpark Integration**: Utilizes PySpark for distributed data processing.
5. **Output**: Displays the count of filtered keywords and their occurrences.

#### Description:

This script demonstrates how to extract subtitles from a YouTube video (`video_id` specified in the script), filter those subtitles based on specific keywords (`specific_keywords` list), and then perform word count analysis using PySpark. The workflow includes:

- Using `YouTubeTranscriptApi` to fetch subtitles.
- Transforming the subtitles into a PySpark DataFrame for efficient data manipulation.
- Defining User Defined Functions (UDFs) to filter and analyze the data.
- Employing PySpark's capabilities to process and analyze distributed data.
- Displaying the results of keyword filtering and word count.

#### Usage:

1. Ensure `pyspark`, `youtube_transcript_api`, and their dependencies are installed (`pip install pyspark youtube_transcript_api`).
2. Replace `video_id` with the YouTube video ID of your choice.
3. Customize `specific_keywords` list based on the keywords you want to filter and analyze.

#### Example Output:

The script outputs the filtered keywords along with their counts in descending order.
