import { GoogleGenAI } from "@google/genai";

const ai = new GoogleGenAI({});

export async function geminiResponse(content: string) {
  const response = await ai.models.generateContent({
    model: "gemini-2.5-flash",
    contents: content,
  });
  return response.text;
}