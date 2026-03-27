import os
from pathlib import Path

# Use the same logic as your Dagster file
ROOT_DIR = Path(__file__).resolve().parent

def check_keys():
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        print("❌ Error: .env file NOT found in root!")
        return

    print(f"✅ Found .env at: {env_path}")
    
    # Load them manually like your script does
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, _ = line.partition("=")
                key = k.strip()
                # Check if it's now in the OS environment
                if key in os.environ:
                    print(f"🔹 Variable '{key}' is LOADED")
                else:
                    print(f"⚠️ Variable '{key}' found in file but NOT in memory")

if __name__ == "__main__":
    check_keys()
