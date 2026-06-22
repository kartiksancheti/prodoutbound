"""Run this after any livekit-plugins-google upgrade to restore the fix."""
path = '/home/prodoutbound/.local/lib/python3.12/site-packages/livekit/plugins/google/realtime/realtime_api.py'
with open(path, 'r') as f:
    src = f.read()

OLD = '            return True\n\n        return False\n'
NEW = '            return True\n\n        # PATCH: gemini-3.1-flash-live-preview sends responses where model_turn\n        # exists but all fields serialize as None in model_dump — check sc directly\n        if (sc := resp.server_content) and sc.model_turn is not None:\n            return True\n        return False\n'

if OLD in src:
    src2 = src.replace(OLD, NEW)
    with open(path, 'w') as f:
        f.write(src2)
    print("✅ Plugin patched successfully")
elif 'PATCH: gemini-3.1-flash-live-preview' in src:
    print("✅ Patch already applied")
else:
    print("❌ Pattern not found — plugin may have changed, check manually")
