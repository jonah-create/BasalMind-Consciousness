"""
BasalMind Intent Taxonomy - Complete Intent Classification System

Purpose: Capture the full spectrum of intents for an AI that:
- Builds software applications and systems
- Executes projects and implements solutions
- Acts as a life coach and supportive buddy
- Designs processes and manages teams
- Patches, deploys, and maintains systems

This taxonomy goes beyond basic conversation - it captures ACTION and EXECUTION.
"""

# Complete Intent Taxonomy for BasalMind
BASALMIND_INTENT_TAXONOMY = {
    "information_seeking": {
        "description": "User seeks information, clarification, or understanding",
        "intents": {
            "asking_question": {
                "keywords": ["?", "how", "what", "why", "when", "where", "who", "can you explain", "tell me", "help me understand"],
                "confidence_boost": 0.2
            },
            "requesting_explanation": {
                "keywords": ["explain", "clarify", "what does", "what is", "define", "meaning of"],
                "confidence_boost": 0.15
            },
            "seeking_advice": {
                "keywords": ["what should i", "should i", "advice", "recommend", "suggest", "thoughts on", "opinion on"],
                "confidence_boost": 0.15
            },
        }
    },

    "building_creating": {
        "description": "User wants to build, create, or develop something NEW",
        "intents": {
            "requesting_build": {
                "keywords": ["build", "create", "develop", "make", "generate", "design", "implement", "write", "code"],
                "confidence_boost": 0.25
            },
            "requesting_feature": {
                "keywords": ["add feature", "new feature", "can you add", "implement", "create a", "build a", "ideas for"],
                "confidence_boost": 0.2
            },
            "requesting_app": {
                "keywords": ["build an app", "create application", "make a program", "develop software", "new project"],
                "confidence_boost": 0.25
            },
            "requesting_design": {
                "keywords": ["design", "architect", "plan out", "structure", "layout", "blueprint"],
                "confidence_boost": 0.18
            },
        }
    },

    "executing_deploying": {
        "description": "User wants to execute, deploy, run, or launch something",
        "intents": {
            "requesting_execution": {
                "keywords": ["run", "execute", "launch", "start", "trigger", "perform", "do it", "make it happen"],
                "confidence_boost": 0.25
            },
            "requesting_deployment": {
                "keywords": ["deploy", "ship", "release", "push to prod", "go live", "roll out"],
                "confidence_boost": 0.25
            },
            "requesting_patch": {
                "keywords": ["patch", "hotfix", "update", "upgrade", "apply fix", "install"],
                "confidence_boost": 0.2
            },
            "requesting_automation": {
                "keywords": ["automate", "schedule", "cron", "trigger", "auto-run", "set up automation"],
                "confidence_boost": 0.2
            },
        }
    },

    "fixing_maintaining": {
        "description": "User wants to fix, debug, or maintain existing systems",
        "intents": {
            "reporting_bug": {
                "keywords": ["bug", "broken", "not working", "error", "crash", "failing", "issue", "down", "outage"],
                "confidence_boost": 0.22
            },
            "requesting_fix": {
                "keywords": ["fix", "repair", "resolve", "debug", "solve", "correct", "troubleshooting", "troubleshoot", "need help"],
                "confidence_boost": 0.22
            },
            "requesting_optimization": {
                "keywords": ["optimize", "improve", "faster", "better performance", "slow", "speed up"],
                "confidence_boost": 0.18
            },
            "requesting_refactor": {
                "keywords": ["refactor", "clean up", "restructure", "improve code", "rewrite"],
                "confidence_boost": 0.18
            },
        }
    },

    "decision_making": {
        "description": "User is making decisions or evaluating options",
        "intents": {
            "making_decision": {
                "keywords": ["decide", "decided", "let's use", "we should", "going with", "chose", "final decision", "decision about"],
                "confidence_boost": 0.25
            },
            "evaluating_options": {
                "keywords": ["pros and cons", "compare", "versus", "vs", "which is better", "evaluate", "options"],
                "confidence_boost": 0.18
            },
            "planning": {
                "keywords": ["plan", "schedule", "roadmap", "next steps", "timeline", "milestone", "strategy"],
                "confidence_boost": 0.15
            },
            "brainstorming": {
                "keywords": ["what if", "maybe", "how about", "idea", "thought", "could we", "alternative", "brainstorm"],
                "confidence_boost": 0.12
            },
        }
    },

    "project_management": {
        "description": "User is managing projects, teams, or processes",
        "intents": {
            "assigning_task": {
                "keywords": ["assign", "can you handle", "take this", "your task", "responsible for", "own this"],
                "confidence_boost": 0.2
            },
            "tracking_progress": {
                "keywords": ["status", "progress", "where are we", "update on", "how's it going", "ETA"],
                "confidence_boost": 0.18
            },
            "setting_deadline": {
                "keywords": ["deadline", "due date", "by when", "finish by", "complete by", "target date"],
                "confidence_boost": 0.18
            },
            "defining_requirements": {
                "keywords": ["requirements", "specs", "needs to", "must have", "should have", "criteria"],
                "confidence_boost": 0.18
            },
        }
    },

    "coaching_support": {
        "description": "User seeks personal guidance, emotional support, or life coaching",
        "intents": {
            "seeking_emotional_support": {
                "keywords": ["feeling", "stressed", "anxious", "overwhelmed", "struggling", "worried", "frustrated"],
                "confidence_boost": 0.22
            },
            "seeking_motivation": {
                "keywords": ["motivate", "encourage", "confidence", "can i do this", "afraid", "nervous", "doubt"],
                "confidence_boost": 0.2
            },
            "seeking_guidance": {
                "keywords": ["guide me", "what should i do", "help me figure out", "lost", "stuck", "don't know", "advice on", "career", "path"],
                "confidence_boost": 0.2
            },
            "reflecting": {
                "keywords": ["thinking about", "reflecting on", "wondering if", "considering", "pondering"],
                "confidence_boost": 0.12
            },
            "celebrating": {
                "keywords": ["did it", "finished", "completed", "success", "won", "achieved", "proud"],
                "confidence_boost": 0.15
            },
        }
    },

    "learning_growth": {
        "description": "User wants to learn, understand, or develop skills",
        "intents": {
            "wanting_to_learn": {
                "keywords": ["learn", "teach me", "how to", "tutorial", "guide", "show me", "train"],
                "confidence_boost": 0.2
            },
            "requesting_documentation": {
                "keywords": ["docs", "documentation", "reference", "manual", "guide", "wiki"],
                "confidence_boost": 0.15
            },
            "requesting_example": {
                "keywords": ["example", "show me", "demo", "sample", "how it works", "walkthrough"],
                "confidence_boost": 0.15
            },
        }
    },

    "knowledge_sharing": {
        "description": "User is sharing information or providing updates",
        "intents": {
            "sharing_knowledge": {
                "keywords": ["FYI", "here's how", "you can", "tip", "this is how", "found out", "discovered"],
                "confidence_boost": 0.12
            },
            "providing_update": {
                "keywords": ["update", "status", "progress", "completed", "done", "finished"],
                "confidence_boost": 0.12
            },
            "documenting": {
                "keywords": ["document", "write down", "note", "record", "log", "capture"],
                "confidence_boost": 0.12
            },
        }
    },

    "collaboration": {
        "description": "User is collaborating, delegating, or coordinating",
        "intents": {
            "requesting_collaboration": {
                "keywords": ["work together", "collaborate", "pair on", "help me with", "join me"],
                "confidence_boost": 0.18
            },
            "delegating": {
                "keywords": ["can you", "please", "would you", "could you handle", "take care of"],
                "confidence_boost": 0.15
            },
            "coordinating": {
                "keywords": ["coordinate", "sync up", "align", "meet", "discuss", "review together"],
                "confidence_boost": 0.15
            },
        }
    },

    "social_emotional": {
        "description": "Social interactions and emotional expressions",
        "intents": {
            "acknowledging": {
                "keywords": ["thanks", "thank you", "got it", "ok", "understood", "makes sense", "👍", "appreciate"],
                "confidence_boost": 0.1
            },
            "agreeing": {
                "keywords": ["yes", "agreed", "correct", "exactly", "absolutely", "definitely", "for sure"],
                "confidence_boost": 0.1
            },
            "disagreeing": {
                "keywords": ["no", "disagree", "but", "however", "actually", "not sure", "i don't think"],
                "confidence_boost": 0.12
            },
            "apologizing": {
                "keywords": ["sorry", "apologize", "my bad", "my fault", "oops", "mistake"],
                "confidence_boost": 0.12
            },
        }
    },
}
