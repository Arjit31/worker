import { Resend } from 'resend';
import dotenv from "dotenv";
dotenv.config();

const resend = new Resend(process.env.RESEND_API_KEY);

export function sendEmail(to: string, body: string, subject: string){
    console.log(to, body); 
    resend.emails.send({
      from: 'onboarding@resend.dev',
      to: to,
      subject: subject,
      html: body
    });
}
