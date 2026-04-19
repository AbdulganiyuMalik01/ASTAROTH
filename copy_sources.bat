@echo off
set CACHE=C:\Users\malik\AppData\Roaming\Claude\local-agent-mode-sessions\e8a1e219-b842-4e41-abdb-543cc465d6ed\ecf566ca-9c73-4e99-bf9c-ef2deb5cb180\.project-cache\019cc82c-9322-77c3-876b-e853de3749ef\docs
set DEST=C:\Users\malik\Documents\Claude\Projects\ASTAROTH

copy "%CACHE%\token_tracker_polling.py" "%DEST%\token_tracker_polling.py"
copy "%CACHE%\token_tracker_webhook_v3.py" "%DEST%\token_tracker_webhook_v3.py"
copy "%CACHE%\token_tracker_webhook_OLD_DISABLED.py" "%DEST%\token_tracker_webhook_OLD_DISABLED.py"
copy "%CACHE%\volume_spike_tracker_v2_1.py" "%DEST%\volume_spike_tracker_v2_1.py"

echo Done.
