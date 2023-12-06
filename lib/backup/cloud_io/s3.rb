require "backup/cloud_io/base"
require "aws-sdk-s3"
require "digest/md5"
require "base64"
require "stringio"

module Backup
  module CloudIO
    class S3 < Base
      MAX_FILE_SIZE       = 1024**3 * 5   # 5 GiB
      MAX_MULTIPART_SIZE  = 1024**4 * 5   # 5 TiB      

      def initialize(bucket:, region:, access_key_id:, secret_access_key:, use_iam_profile:, chunk_size:, custom_endpoint:)
        @bucket = bucket
        @region = region
        @access_key_id = access_key_id
        @secret_access_key = secret_access_key
        @use_iam_profile = use_iam_profile
        @chunk_size = chunk_size
        @custom_endpoint = custom_endpoint
      end
    
      # The Syncer may call this method in multiple threads.
      # However, #objects is always called prior to multithreading.
      def upload(src, dest)
        file_size = File.size(src)
        chunk_bytes = @chunk_size * 1024**2
    
        if chunk_bytes > 0 && file_size > chunk_bytes
          raise FileSizeError, "File Too Large\nFile: #{src}\nSize: #{file_size}\nMax Multipart Upload Size is #{MAX_MULTIPART_SIZE} (5 TiB)" if file_size > MAX_MULTIPART_SIZE
          
          s3 = s3_resource
          upload = s3.create_multipart_upload(bucket: @bucket, key: dest)
          upload_id = upload.upload_id
          parts = []

          total_parts = (file_size / chunk_bytes.to_f).ceil
          progress = (0.1..0.9).step(0.1).map { |n| (total_parts * n).floor }

          File.open(src, 'rb') do |file|
            part_number = 0
            while part = file.read(chunk_bytes)
              part_number = parts.size + 1
              response = s3.upload_part(bucket: @bucket, key: dest, upload_id: upload_id, part_number: part_number, body: part)
              parts << { etag: response.etag, part_number: part_number }

              if i = progress.rindex(part_number)
                Logger.info "\s\s...#{i + 1}0% Complete..."
              end
            end
          end

          s3.complete_multipart_upload(bucket: @bucket, key: dest, upload_id: upload_id, multipart_upload: { parts: parts })
          
        else
          raise FileSizeError, "File Too Large\nFile: #{src}\nSize: #{file_size}\nMax File Size is #{MAX_FILE_SIZE} (5 GiB)" if file_size > MAX_FILE_SIZE
    
          # put_object(src, dest)
        end
      end
    
      # Returns all objects in the bucket with the given prefix.
      #
      # - #get_bucket returns a max of 1000 objects per request.
      # - Returns objects in alphabetical order.
      # - If marker is given, only objects after the marker are in the response.
      def objects(prefix)
        s3 = s3_resource
    
        objects = []
        prefix = prefix.chomp("/")
        bucket = s3.bucket(@bucket)
    
        bucket.objects(prefix: prefix + "/").each do |s3_object|
          objects << S3Object.new(self, s3_object.key, s3_object.etag, s3_object.storage_class)
        end
    
        objects
      end
    
      private

      def s3_resource
        options = {
          region: @region,
          access_key_id: @access_key_id,
          secret_access_key: @secret_access_key,
        }
    
        options[:endpoint] = @custom_endpoint if @custom_endpoint
    
        Aws::S3::Client.new(options)
      end     
    
      class S3Object
        attr_reader :key, :etag, :storage_class
    
        def initialize(s3_uploader, key, etag, storage_class)
          @s3_uploader = s3_uploader
          @key = key
          @etag = etag
          @storage_class = storage_class
        end
      end
    end
  end
end
