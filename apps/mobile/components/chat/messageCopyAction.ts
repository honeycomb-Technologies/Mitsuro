export type MessageCopyFeedback = "Copied" | "Could not copy" | null;

/** Owns feedback for one message; clipboard writes themselves are not cancelable. */
export function createMessageCopyAction(
  writeText: (text: string) => Promise<void | boolean>,
  showFeedback: (feedback: MessageCopyFeedback) => void,
) {
  let generation = 0;
  let feedbackTimer: ReturnType<typeof setTimeout> | undefined;

  const cancel = () => {
    generation++;
    if (feedbackTimer !== undefined) clearTimeout(feedbackTimer);
    feedbackTimer = undefined;
  };

  return {
    cancel,
    async copy(text: string): Promise<boolean> {
      if (!text) return false;
      cancel();
      const request = generation;
      showFeedback(null);
      let copied = false;
      try {
        // Web resolves void; Expo's native wrapper returns a success boolean.
        copied = (await writeText(text)) !== false;
      } catch {
        copied = false;
      }
      if (request !== generation) return false;
      showFeedback(copied ? "Copied" : "Could not copy");
      feedbackTimer = setTimeout(() => {
        feedbackTimer = undefined;
        if (request === generation) showFeedback(null);
      }, copied ? 1200 : 3000);
      return copied;
    },
  };
}
