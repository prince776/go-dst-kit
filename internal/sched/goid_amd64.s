#include "textflag.h"

// func getg() uint64
TEXT ·getg(SB),NOSPLIT,$0-8
	MOVQ (TLS), AX
	MOVQ AX, ret+0(FP)
	RET
