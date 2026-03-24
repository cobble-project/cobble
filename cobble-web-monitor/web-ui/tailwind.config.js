/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{vue,js,ts}'],
  theme: {
    extend: {
      colors: {
        coffee: {
          900: '#17120f',
          800: '#211812',
          700: '#2b1f18',
          600: '#3a2a21',
          500: '#4b3529',
          300: '#bfa089',
          200: '#dfcdbf',
        },
      },
    },
  },
  plugins: [],
}
