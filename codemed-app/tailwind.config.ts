import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        bg: "#0A0F1A",
        surface: "#10151F",
        card: "#141B28",
        card2: "#192030",
        green: "#00D4A0",
        green2: "#00B386",
        gold: "#F6AD3C",
        red: "#FC5A5A",
        purple: "#9B8AFB",
        gray: "#6B8299",
        "gray-light": "#9BAFC4",
        white: "#EDF2F7",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        mono: ["IBM Plex Mono", "monospace"],
      },
      backdropBlur: {
        xs: "2px",
      },
    },
  },
  plugins: [],
};

export default config;
