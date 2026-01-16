import os
import json
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import uuid
import mimetypes

API_URL = "https://api.topazlabs.com/video/"
API_EXPRESS_URL = "https://api.topazlabs.com/video/express"
KEY_FILE = "key.txt"
INPUT_FOLDER = "input"
OUTPUT_FOLDER = "output"
LOG_FILE = "processing_log.json"
MAX_CONCURRENT_TASKS = 5
RETRY_LIMIT = 1

VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.webm', '.m4v'}

class VideoProcessor:
    def __init__(self):
        self.api_key = self.load_api_key()
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        self.log_data = {
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "total_videos": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "results": []
        }
        
    def load_api_key(self) -> str:
        key_path = Path(KEY_FILE)
        if not key_path.exists():
            raise FileNotFoundError(f"API key file not found: {KEY_FILE}")
        return key_path.read_text().strip()
    
    def ensure_folders(self):
        Path(INPUT_FOLDER).mkdir(exist_ok=True)
        Path(OUTPUT_FOLDER).mkdir(exist_ok=True)
    
    def get_video_files(self) -> List[Path]:
        input_path = Path(INPUT_FOLDER)
        video_files = []
        for file in input_path.iterdir():
            if file.is_file() and file.suffix.lower() in VIDEO_EXTENSIONS:
                video_files.append(file)
        return sorted(video_files)
    
    def is_already_processed(self, input_file: Path) -> bool:
        output_file = Path(OUTPUT_FOLDER) / input_file.name
        return output_file.exists()
    
    async def get_video_info(self, file_path: Path) -> Optional[Dict]:
        try:
            import subprocess
            result = subprocess.run(
                ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', str(file_path)],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                video_stream = next((s for s in data.get('streams', []) if s['codec_type'] == 'video'), None)
                if video_stream:
                    format_info = data.get('format', {})
                    return {
                        'width': int(video_stream.get('width', 0)),
                        'height': int(video_stream.get('height', 0)),
                        'duration': float(format_info.get('duration', 0)),
                        'size': int(format_info.get('size', 0)),
                        'frame_rate': eval(video_stream.get('r_frame_rate', '24/1')),
                        'frame_count': int(video_stream.get('nb_frames', 0))
                    }
        except Exception as e:
            print(f"Warning: Could not get video info for {file_path.name}, using defaults: {e}")
        
        file_size = file_path.stat().st_size
        return {
            'width': 1920,
            'height': 1080,
            'duration': 10.0,
            'size': file_size,
            'frame_rate': 24,
            'frame_count': 240
        }
    
    async def submit_video_job(self) -> Dict:
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        }
        
        payload = {
            "source": {
                "container": "mp4"
            },
            "filters": [
                {
                    "model": "prob-4"
                }
            ],
            "output": {
                "frameRate": 24,
                "audioTransfer": "Copy",
                "audioCodec": "AAC",
                "videoEncoder": "H265",
                "videoProfile": "Main",
                "dynamicCompressionLevel": "Mid",
                "resolution": {
                    "width": 1920,
                    "height": 1078
                }
            }
        }
        
        async with self.session.post(API_EXPRESS_URL, headers=headers, json=payload) as response:
            response_data = await response.json()
            if response.status != 200:
                raise Exception(f"API error: {response.status} - {response_data}")
            return response_data
    
    async def upload_video(self, file_path: Path, upload_url: str) -> bool:
        async with aiofiles.open(file_path, 'rb') as f:
            video_data = await f.read()
        
        headers = {'Content-Type': 'video/mp4'}
        async with self.session.put(upload_url, data=video_data, headers=headers) as response:
            return response.status in [200, 201, 204]
    
    async def check_job_status(self, request_id: str) -> Dict:
        headers = {
            'Accept': 'application/json',
            'X-API-Key': self.api_key
        }
        
        url = f"{API_URL}{request_id}/status"
        async with self.session.get(url, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"Status check failed: {response.status}")
            return await response.json()
    
    async def download_result(self, download_url: str, temp_file: Path) -> bool:
        async with self.session.get(download_url) as response:
            if response.status != 200:
                return False
            
            async with aiofiles.open(temp_file, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
            return True
    
    async def process_single_video(self, input_file: Path, retry_count: int = 0) -> Dict:
        result = {
            "filename": input_file.name,
            "status": "unknown",
            "request_id": None,
            "error": None,
            "retry_count": retry_count,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            if self.is_already_processed(input_file):
                result["status"] = "skipped"
                result["error"] = "Already processed"
                self.log_data["skipped"] += 1
                print(f"⏭️  Skipped: {input_file.name} (already processed)")
                return result
            
            print(f"🎬 Processing: {input_file.name}")
            
            job_response = await self.submit_video_job()
            request_id = job_response.get('requestId')
            upload_urls = job_response.get('uploadUrls', [])
            result["request_id"] = request_id
            
            if not request_id:
                raise Exception("No request ID received from API")
            if not upload_urls or len(upload_urls) == 0:
                raise Exception("No upload URLs received from API")
            
            upload_url = upload_urls[0]
            
            print(f"   ✓ Job created: {request_id}")
            
            print(f"   ⬆️  Uploading video...")
            if not await self.upload_video(input_file, upload_url):
                raise Exception("Video upload failed")
            print(f"   ✓ Upload complete, processing started")
            
            max_wait = 3600
            wait_interval = 10
            elapsed = 0
            
            while elapsed < max_wait:
                await asyncio.sleep(wait_interval)
                elapsed += wait_interval
                
                status_response = await self.check_job_status(request_id)
                status = status_response.get('status')
                
                if status == 'complete':
                    download_url = status_response.get('download', {}).get('url')
                    if not download_url:
                        raise Exception("No download URL in completed response")
                    
                    temp_uuid = str(uuid.uuid4())
                    temp_file = Path(OUTPUT_FOLDER) / f"{temp_uuid}.mp4"
                    final_file = Path(OUTPUT_FOLDER) / input_file.name
                    
                    print(f"   ⬇️  Downloading result...")
                    if await self.download_result(download_url, temp_file):
                        temp_file.rename(final_file)
                        result["status"] = "success"
                        self.log_data["successful"] += 1
                        print(f"   ✅ Success: {input_file.name}")
                        return result
                    else:
                        raise Exception("Download failed")
                
                elif status == 'failed':
                    error_msg = status_response.get('error', 'Unknown error')
                    print(f"   🔍 Full API response: {json.dumps(status_response, indent=2)}")
                    raise Exception(f"Job failed: {error_msg}")
                
                elif status in ['requested', 'accepted', 'initializing', 'preprocessing', 'processing', 'postprocessing']:
                    progress = status_response.get('progress', 0)
                    print(f"   ⏳ Progress: {progress}% - Status: {status}")
                else:
                    print(f"   ℹ️  Status: {status}")
            
            raise Exception(f"Timeout after {max_wait}s")
            
        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Error: {input_file.name} - {error_msg}")
            
            if retry_count < RETRY_LIMIT:
                print(f"   🔄 Retrying ({retry_count + 1}/{RETRY_LIMIT})...")
                await asyncio.sleep(5)
                return await self.process_single_video(input_file, retry_count + 1)
            
            result["status"] = "failed"
            result["error"] = error_msg
            self.log_data["failed"] += 1
            return result
    
    async def process_video_with_semaphore(self, input_file: Path) -> Dict:
        async with self.semaphore:
            return await self.process_single_video(input_file)
    
    async def process_all_videos(self):
        self.ensure_folders()
        
        video_files = self.get_video_files()
        self.log_data["total_videos"] = len(video_files)
        
        if not video_files:
            print("⚠️  No video files found in input folder")
            return
        
        print(f"📊 Found {len(video_files)} video(s) to process")
        print(f"⚙️  Max concurrent tasks: {MAX_CONCURRENT_TASKS}")
        print(f"🔄 Retry limit: {RETRY_LIMIT}")
        print("-" * 60)
        
        timeout = aiohttp.ClientTimeout(
            total=7200,
            connect=60,
            sock_read=300
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            
            tasks = [self.process_video_with_semaphore(video_file) for video_file in video_files]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    self.log_data["results"].append({
                        "filename": "unknown",
                        "status": "failed",
                        "error": str(result),
                        "timestamp": datetime.now().isoformat()
                    })
                    self.log_data["failed"] += 1
                else:
                    self.log_data["results"].append(result)
        
        self.log_data["end_time"] = datetime.now().isoformat()
        
        await self.save_log()
        self.print_summary()
    
    async def save_log(self):
        async with aiofiles.open(LOG_FILE, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(self.log_data, indent=2, ensure_ascii=False))
        print(f"\n📝 Log saved to: {LOG_FILE}")
    
    def print_summary(self):
        print("\n" + "=" * 60)
        print("📊 PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Total videos:     {self.log_data['total_videos']}")
        print(f"✅ Successful:    {self.log_data['successful']}")
        print(f"❌ Failed:        {self.log_data['failed']}")
        print(f"⏭️  Skipped:       {self.log_data['skipped']}")
        print("=" * 60)
        
        if self.log_data['failed'] > 0:
            print("\n❌ Failed videos:")
            for result in self.log_data['results']:
                if result['status'] == 'failed':
                    print(f"   - {result['filename']}: {result['error']}")

async def main():
    try:
        processor = VideoProcessor()
        await processor.process_all_videos()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
