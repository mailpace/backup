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
        Logger.info "Custom endpoint in cloud io: #{custom_endpoint}"

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
    
          chunk_bytes = adjusted_chunk_bytes(chunk_bytes, file_size)
          upload_id = initiate_multipart(dest)
          parts = upload_parts(src, dest, upload_id, chunk_bytes, file_size)
          complete_multipart(dest, upload_id, parts)
        else
          raise FileSizeError, "File Too Large\nFile: #{src}\nSize: #{file_size}\nMax File Size is #{MAX_FILE_SIZE} (5 GiB)" if file_size > MAX_FILE_SIZE
    
          put_object(src, dest)
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
    
      # Used by Object to fetch metadata if needed.
      def head_object(object)
        s3 = s3_resource
        bucket = s3.bucket(@bucket)
        s3_object = bucket.object(object.key)
        s3_object.head
      end
    
      # Delete object(s) from the bucket.
      #
      # - Called by the Storage (with objects) and the Syncer (with keys)
      # - Deletes 1000 objects per request.
      # - Missing objects will be ignored.
      def delete(objects_or_keys)
        s3 = s3_resource
        bucket = s3.bucket(@bucket)
    
        keys = Array(objects_or_keys).map { |obj| obj.is_a?(S3Object) ? obj.key : obj }
    
        until keys.empty?
          keys_partial = keys.slice!(0, 1000)
          delete_objects(bucket, keys_partial)
        end
      end
    
      private

      def s3_resource
        options = {
          region: @region,
          access_key_id: @access_key_id,
          secret_access_key: @secret_access_key,
        }

        Logger.info "access_key_id: #{@access_key_id}"
        Logger.info "secret_access_key: #{@secret_access_key}"
    
        options[:endpoint] = @custom_endpoint if @custom_endpoint
    
        Aws::S3::Resource.new(options)
      end

      def metadata
        @metadata ||= begin
          head_object = @s3.head_object(self)
          head_object.respond_to?(:headers) ? head_object.headers : {}
        end
      end
    
      def delete_objects(bucket, keys)
        bucket.objects.delete(keys: keys, quiet: true)
      end
    
      def put_object(src, dest)
        s3 = s3_resource
        bucket = s3.bucket(@bucket)
    
        md5 = Base64.encode64(Digest::MD5.file(src).digest).chomp
        options = { content_md5: md5 }
    
        File.open(src, "rb") do |file|
          bucket.object(dest).put(body: file, content_md5: md5, **options)
        end
      end
    
      def initiate_multipart(dest)
        Logger.info "\s\sInitiate Multipart '#{@bucket}/#{dest}'"
    
        s3 = s3_resource
        bucket = s3.bucket(@bucket)
    
        resp = bucket.object(dest).create_multipart_upload
        resp.upload_id
      end
    
      # Each part's MD5 is sent to verify the transfer.
      # AWS will concatenate all parts into a single object
      # once the multipart upload is completed.
      def upload_parts(src, dest, upload_id, chunk_bytes, file_size)
        total_parts = (file_size / chunk_bytes.to_f).ceil
        progress = (0.1..0.9).step(0.1).map { |n| (total_parts * n).floor }
        Logger.info "\s\sUploading #{total_parts} Parts..."
    
        s3 = s3_resource
        bucket = s3.bucket(@bucket)
    
        parts = []
        File.open(src, "rb") do |file|
          part_number = 0
          while data = file.read(chunk_bytes)
            part_number += 1
            md5 = Base64.encode64(Digest::MD5.digest(data)).chomp
    
            resp = bucket.object(dest).upload_part(
              body: StringIO.new(data),
              part_number: part_number,
              upload_id: upload_id,
              content_md5: md5
            )
            parts << resp.etag
    
            if i = progress.rindex(part_number)
              Logger.info "\s\s...#{i + 1}0% Complete..."
            end
          end
        end
    
        parts
      end
    
      def complete_multipart(dest, upload_id, parts)
        Logger.info "\s\sComplete Multipart '#{@bucket}/#{dest}'"
    
        s3 = s3_resource
        bucket = s3.bucket(@bucket)
    
        bucket.object(dest).complete_multipart_upload(upload_id: upload_id, multipart_upload: { parts: parts })
      end
       
      def adjusted_chunk_bytes(chunk_bytes, file_size)
        return chunk_bytes if file_size / chunk_bytes.to_f <= 10_000
    
        mb = orig_mb = chunk_bytes / 1024**2
        mb += 1 until file_size / (1024**2 * mb).to_f <= 10_000
        Logger.warn Error.new(<<-EOS)
          Chunk Size Adjusted
          Your original #chunk_size of #{orig_mb} MiB has been adjusted
          to #{mb} MiB in order to satisfy the limit of 10,000 chunks.
          To enforce your chosen #chunk_size, you should use the Splitter.
          e.g. split_into_chunks_of #{mb * 10_000} (#chunk_size * 10_000)
        EOS
        1024**2 * mb
      end
    
      class S3Object
        attr_reader :key, :etag, :storage_class
    
        def initialize(s3_uploader, key, etag, storage_class)
          @s3_uploader = s3_uploader
          @key = key
          @etag = etag
          @storage_class = storage_class
        end

        private

        def metadata
          @metadata ||= @s3_uploader.head_object(self).headers
        end
      end
    end
  end
end
