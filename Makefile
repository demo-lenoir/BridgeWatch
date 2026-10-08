.PHONY: verify-phase0 verify-phase1 verify-phase2 verify-phase3 verify-phase4 verify-phase5 demo benchmark-smoke benchmark clean-clone-verify local-release

verify-phase0:
	python3 scripts/verify_phase0.py

verify-phase1:
	python3 scripts/verify_phase1.py

verify-phase2:
	python3 scripts/verify_phase2.py

verify-phase3:
	python3 scripts/verify_phase3.py

verify-phase4:
	python3 scripts/verify_phase4.py

demo:
	python3 scripts/phase5_local.py demo

benchmark-smoke:
	python3 scripts/phase5_local.py benchmark-smoke

benchmark:
	python3 scripts/phase5_local.py benchmark

clean-clone-verify:
	python3 scripts/clean_clone_verify.py

local-release:
	python3 scripts/release_phase5.py

verify-phase5:
	python3 scripts/verify_phase5.py
