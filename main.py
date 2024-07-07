from pyspark.sql import SparkSession
from youtube_transcript_api import YouTubeTranscriptApi
from pyspark.sql.functions import udf, explode, split, col
from pyspark.sql.types import StringType

def get_transcript(video_id):
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
        return ' '.join([entry['text'] for entry in transcript_list])
    except Exception as e:
        print(f"Error retrieving transcript: {e}")
        return ""

def stream_subtitles(video_id):
    transcript = get_transcript(video_id)
    if transcript:
        return transcript.split('. ')
    else:
        return []

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("YouTube-Subtitle-Streaming") \
        .master("local[3]") \
        .getOrCreate()

    video_id = "PErfbXcPzEw"  # video id youtube


    subtitles_rdd = spark.sparkContext.parallelize(stream_subtitles(video_id))
    subtitles_df = subtitles_rdd.map(lambda x: (x,)).toDF(["subtitle"])

    # keyword
    specific_keywords = ["election", "London", "to", "filter"]


    def filter_subtitles(subtitle):
        for sentence in specific_keywords:
            if sentence in subtitle:
                return subtitle
        return None

    filter_subtitles_udf = udf(filter_subtitles, StringType())


    filtered_df = subtitles_df.withColumn("filtered_subtitle", filter_subtitles_udf(subtitles_df.subtitle))
    result_df = filtered_df.filter(filtered_df.filtered_subtitle.isNotNull())


    def filter_words(word):
        if word in specific_keywords:
            return word
        return None

    filter_words_udf = udf(filter_words, StringType())


    words_df = result_df.select(explode(split(col("filtered_subtitle"), " ")).alias("word"))
    filtered_words_df = words_df.withColumn("filtered_word", filter_words_udf(words_df.word))
    result_words_df = filtered_words_df.filter(filtered_words_df.filtered_word.isNotNull())


    word_count_df = result_words_df.groupBy("filtered_word").count().orderBy("count", ascending=False)


    print("Filtered Words and their counts:")
    word_count_df.show(truncate=False)

    spark.stop()